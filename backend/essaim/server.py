"""Processus principal : simulation à pas fixe + producteur/consommateur Kafka + WebSocket pour Godot.

    python -m essaim.server [--mode llm|regles] [--seed N]
"""
import argparse
import asyncio
import json
import logging
import time

import websockets
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from websockets.asyncio.server import broadcast, serve

from .config import load_config
from .db import Database
from .llm import OllamaClient, build_prompt
from .metrics import RunMetrics
from .world import World

log = logging.getLogger("essaim")


class App:
    def __init__(self, cfg, mode: str, seed: int):
        self.cfg, self.mode = cfg, mode
        self.db = Database(cfg.sqlite_path)
        self.ollama = OllamaClient(cfg.ollama_base_url, cfg.ollama_model, cfg.ollama_timeout_s, cfg.ollama_num_ctx)
        self.clients = set()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self.metrics: dict[str, RunMetrics] = {}
        self.kafka = {"producer": "démarrage", "consumer": "démarrage", "published": 0, "lost": 0,
                      "last_consumed_wall": None}
        self.llm_tasks: set[asyncio.Task] = set()
        self.last_fallback_reason = None
        self.kafka_healthy = None
        self.new_world(seed)

    # ------------------------------------------------------------------ monde
    def new_world(self, seed: int) -> None:
        llm_bees = {0} if self.mode == "llm" else set()
        self.world = World(seed, llm_bees, meta={
            "mode": self.mode, "model": self.cfg.ollama_model if self.mode == "llm" else None,
            "consensus_min_bees": self.cfg.consensus_min_bees, "consensus_hold_s": self.cfg.consensus_hold_s})
        self.paused = self.ended = False
        log.info("nouveau run %s (graine %s, mode %s)", self.world.run_id, seed, self.mode)
        self.flush_world()

    def flush_world(self) -> None:
        events, requests, decisions = self.world.drain()
        for e in events:
            try:
                self.queue.put_nowait(e)
            except asyncio.QueueFull:
                self.kafka["lost"] += 1
                log.error("tampon Kafka plein : événement %s perdu", e["event_type"])
        for d in decisions:
            self.db.insert_decision(d)
        for req in requests:
            task = asyncio.get_running_loop().create_task(self.llm_decision(req))
            self.llm_tasks.add(task)
            task.add_done_callback(self.llm_tasks.discard)

    async def llm_decision(self, req: dict) -> None:
        allowed = [o["letter"] for o in req["options"]]
        if self.ollama.status != "prêt":
            info = {"letter": None, "error": f"Ollama {self.ollama.status} ({self.cfg.ollama_model})", "latency_ms": 0}
        else:
            bee_view = self.world.bees[req["bee"]].to_view() if req["run_id"] == self.world.run_id else None
            if bee_view is None:
                return
            info = await self.ollama.decide(build_prompt(bee_view, req), allowed)
        source = "llm" if info["letter"] else "fallback"
        letter = info.pop("letter")
        if source == "fallback":
            info["fallback_reason"] = info.get("error")
            if info.get("error") != self.last_fallback_reason:  # chaque décision est en base ; on ne log que les changements
                log.warning("abeille %s : repli à règles (%s)", req["bee"], info.get("error"))
            self.last_fallback_reason = info.get("error")
        result = self.world.resolve_llm(req["decision_id"], req["run_id"], letter, source, info)
        if result != "appliquée":
            log.info("décision %s %s", req["decision_id"], result)
            self.db.insert_decision({"run_id": req["run_id"], "decision_id": req["decision_id"], "agent_id": req["bee"],
                                     "kind": req["kind"], "requested_at": req["t"], "sim_time": self.world.time,
                                     "options": req["options"], "choice": letter, "source": "ignorée",
                                     **info, "error": result})
        self.flush_world()

    def end_run(self, reason: str) -> None:
        if not self.ended:
            self.world.emit(None, "run_ended", {"reason": reason})
            self.ended = True
            self.flush_world()

    async def sim_loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_tick = last_snapshot = last_status = loop.time()
        while True:
            if not self.paused and not self.ended:
                self.world.step(self.cfg.sim_dt)
                if self.world.time >= self.cfg.sim_max_time:
                    self.end_run("durée maximale atteinte")
                self.flush_world()
            now = loop.time()
            if now - last_snapshot >= 0.1:
                last_snapshot = now
                self.db.commit()
                self.send(self.snapshot_msg())
            if now - last_status >= 1.0:
                last_status = now
                status = self.status_msg()
                self.log_kafka_health(status)
                self.send(status)
            next_tick = max(next_tick + self.cfg.sim_dt / self.cfg.sim_speed, now - 0.5)
            await asyncio.sleep(max(0.0, next_tick - loop.time()))

    # ------------------------------------------------------------------ Kafka
    async def publisher(self) -> None:
        producer, event = None, None
        while True:
            if producer is None:
                producer = AIOKafkaProducer(bootstrap_servers=self.cfg.kafka_bootstrap, acks="all",
                                            request_timeout_ms=5000, retry_backoff_ms=1000)
                try:
                    await producer.start()
                    self.kafka["producer"] = "ok"
                except Exception as exc:  # noqa: BLE001
                    self.kafka["producer"] = f"PANNE: {type(exc).__name__}"
                    await producer.stop()
                    producer = None
                    await asyncio.sleep(3)
                    continue
            if event is None:
                event = await self.queue.get()
            try:
                await asyncio.wait_for(producer.send_and_wait(
                    self.cfg.kafka_topic, json.dumps(event, ensure_ascii=False).encode(),
                    key=event["run_id"].encode()), timeout=10)
                self.kafka["published"] += 1
                event = None  # sinon on retente le même événement : l'ordre est préservé
            except Exception as exc:  # noqa: BLE001
                self.kafka["producer"] = f"PANNE: {type(exc).__name__}"
                log.error("publication Kafka échouée (%s) ; nouvelle tentative", exc)
                await producer.stop()
                producer = None

    async def consumer(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(self.cfg.kafka_topic, bootstrap_servers=self.cfg.kafka_bootstrap,
                                        group_id=self.cfg.kafka_group, enable_auto_commit=False,
                                        auto_offset_reset="earliest", retry_backoff_ms=1000)
            try:
                await consumer.start()
                self.kafka["consumer"] = "ok"
                while True:
                    batch = await consumer.getmany(timeout_ms=500)
                    for tp, messages in batch.items():
                        for msg in messages:
                            self.consume(json.loads(msg.value), tp.partition, msg.offset)
                    if batch:
                        self.db.commit()          # d'abord SQLite...
                        await consumer.commit()   # ...puis l'offset : au pire on relit, jamais on ne perd
                        self.kafka["last_consumed_wall"] = time.time()
            except Exception as exc:  # noqa: BLE001
                self.kafka["consumer"] = f"PANNE: {type(exc).__name__}"
                log.error("consommateur Kafka : %s", exc)
                await asyncio.sleep(3)
            finally:
                await consumer.stop()

    def consume(self, event: dict, partition: int | None = None, offset: int | None = None) -> bool:
        """Sauvegarde l'événement (sans doublon) et met à jour les indicateurs de son run."""
        run_id = event["run_id"]
        inserted = self.db.insert_event(event, partition, offset)
        m = self.metrics.get(run_id)
        if m is None:  # run inconnu en mémoire (ex. redémarrage) : on reconstruit depuis SQLite
            m = self.metrics[run_id] = RunMetrics(run_id, self.cfg.consensus_min_bees, self.cfg.consensus_hold_s)
            for e in self.db.run_events(run_id):
                m.apply(e)
        applied = m.apply(event)
        if event["event_type"] == "run_ended" and (inserted or applied):
            self.db.end_run(run_id, event["payload"]["reason"], event["sim_time"], m.to_dict())
        if run_id == self.world.run_id:
            self.send({"type": "metrics", **m.to_dict()})
        return inserted

    # ------------------------------------------------------------------ WebSocket
    def snapshot_msg(self) -> dict:
        return {"type": "snapshot", "mode": self.mode, "paused": self.paused, "ended": self.ended,
                "llm_bees": sorted(self.world.llm_bees), **self.world.snapshot()}

    def status_msg(self) -> dict:
        consumed = self.kafka["last_consumed_wall"]
        return {"type": "status", "run_id": self.world.run_id, "sim_time": round(self.world.time, 2),
                "python": "ok", "mode": self.mode,
                "kafka_producer": self.kafka["producer"], "kafka_consumer": self.kafka["consumer"],
                "kafka_pending": self.queue.qsize(), "kafka_published": self.kafka["published"],
                "kafka_lost": self.kafka["lost"],
                "kafka_last_consumed_s": round(time.time() - consumed, 1) if consumed else None,
                "ollama": self.ollama.status, "ollama_model": self.cfg.ollama_model,
                "llm_pending": len(self.llm_tasks)}

    def log_kafka_health(self, status: dict) -> None:
        """Une ligne de log à chaque passage panne <-> rétabli (les logs internes d'aiokafka sont coupés)."""
        if "démarrage" in (status["kafka_producer"], status["kafka_consumer"]):
            return
        stale = status["kafka_last_consumed_s"] is None or status["kafka_last_consumed_s"] > 5
        healthy = status["kafka_producer"] == "ok" and status["kafka_consumer"] == "ok" and not stale
        if healthy != self.kafka_healthy:
            self.kafka_healthy = healthy
            if healthy:
                log.info("Kafka rétabli (publiés %d, perdus %d)", status["kafka_published"], status["kafka_lost"])
            else:
                log.error("Kafka en PANNE : producteur=%s consommateur=%s, %d événements en attente, "
                          "dernier message consommé il y a %ss", status["kafka_producer"], status["kafka_consumer"],
                          status["kafka_pending"], status["kafka_last_consumed_s"])

    def send(self, msg: dict) -> None:
        if self.clients:
            broadcast(self.clients, json.dumps(msg, ensure_ascii=False))

    async def ws_handler(self, ws) -> None:
        self.clients.add(ws)
        log.info("client WebSocket connecté (%d)", len(self.clients))
        try:
            await ws.send(json.dumps(self.snapshot_msg(), ensure_ascii=False))
            await ws.send(json.dumps(self.status_msg(), ensure_ascii=False))
            m = self.metrics.get(self.world.run_id)
            if m:
                await ws.send(json.dumps({"type": "metrics", **m.to_dict()}, ensure_ascii=False))
            async for raw in ws:
                reply = self.command(json.loads(raw))
                await ws.send(json.dumps({"type": "ack", **reply}, ensure_ascii=False))
        except (websockets.ConnectionClosed, json.JSONDecodeError) as exc:
            log.info("client WebSocket fermé : %s", type(exc).__name__)
        finally:
            self.clients.discard(ws)

    def command(self, cmd: dict) -> dict:
        name = cmd.get("cmd")
        if name == "pause" and not self.paused and not self.ended:
            self.paused = True
            self.world.emit(None, "sim_paused", {})
        elif name == "resume" and self.paused:
            self.paused = False
            self.world.emit(None, "sim_resumed", {})
        elif name == "reset":
            self.end_run("réinitialisation")
            if cmd.get("mode") in ("llm", "regles"):
                self.mode = cmd["mode"]
            self.new_world(int(cmd.get("seed", self.world.seed)))
        elif name == "set_site_accessible":
            self.world.set_site_accessible(cmd["site"], bool(cmd["accessible"]))
        elif name == "snapshot":
            pass
        else:
            return {"cmd": name, "ok": False, "error": "commande inconnue ou sans effet"}
        self.flush_world()
        self.send(self.snapshot_msg())
        return {"cmd": name, "ok": True, "run_id": self.world.run_id}

    async def ollama_watch(self) -> None:
        while True:
            before = self.ollama.status
            status = await self.ollama.check()
            if status != before:
                (log.info if status == "prêt" else log.warning)("Ollama : %s (%s)", status, self.cfg.ollama_model)
            await asyncio.sleep(30)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["llm", "regles"])
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    cfg = load_config()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)  # remplacé par log_kafka_health()
    for noisy in ("httpx", "websockets.server"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    app = App(cfg, args.mode or cfg.mode, args.seed if args.seed is not None else cfg.seed)
    await app.ollama.check()
    if app.mode == "llm" and app.ollama.status != "prêt":
        log.warning("Ollama %s pour %s : l'abeille 0 utilisera le repli à règles (signalé 'fallback').",
                    app.ollama.status, cfg.ollama_model)
    async with serve(app.ws_handler, cfg.ws_host, cfg.ws_port):
        log.info("WebSocket sur ws://%s:%s", cfg.ws_host, cfg.ws_port)
        await asyncio.gather(app.sim_loop(), app.publisher(), app.consumer(), app.ollama_watch())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
