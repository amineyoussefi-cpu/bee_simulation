"""WebSocket (sans Kafka) et chaîne Kafka -> SQLite -> indicateurs (si Kafka répond)."""
import asyncio
import dataclasses
import json
import socket
import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from essaim.config import load_config
from essaim.server import App
from essaim.world import World


def make_app(tmp_path, **overrides) -> App:
    cfg = dataclasses.replace(load_config(), sqlite_path=tmp_path / "test.sqlite3", **overrides)
    return App(cfg, "regles", seed=3)


def kafka_up() -> bool:
    host, port = load_config().kafka_bootstrap.split(":")
    try:
        socket.create_connection((host, int(port)), timeout=1).close()
        return True
    except OSError:
        return False


def test_websocket_snapshot_commands_and_reconnect(tmp_path):
    async def scenario():
        app = make_app(tmp_path)
        sim = asyncio.create_task(app.sim_loop())
        async with serve(app.ws_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}"
            async with connect(url) as ws:
                first = json.loads(await ws.recv())
                assert first["type"] == "snapshot" and len(first["bees"]) == 10 and first["run_id"]
                await ws.send(json.dumps({"cmd": "pause"}))
                msgs = [json.loads(await ws.recv()) for _ in range(5)]
                assert any(m["type"] == "ack" and m["ok"] for m in msgs)
                assert app.paused
                t = app.world.time
                await asyncio.sleep(0.3)
                assert app.world.time == t  # en pause, le temps simulé ne bouge plus
                old_run = app.world.run_id
                await ws.send(json.dumps({"cmd": "reset", "seed": 9}))
                await asyncio.sleep(0.2)
                assert app.world.run_id != old_run and app.world.seed == 9 and not app.paused
            # reconnexion : un nouveau client reçoit immédiatement un snapshot complet du run courant
            async with connect(url) as ws:
                snap = json.loads(await ws.recv())
                assert snap["type"] == "snapshot" and snap["run_id"] == app.world.run_id
        sim.cancel()
        types = [e["event_type"] for e in app.queue._queue]
        assert "sim_paused" in types and "run_ended" in types and types.count("run_started") == 2

    asyncio.run(scenario())


def test_websocket_stays_responsive_during_slow_llm_call(tmp_path, stub_ollama):
    url, stub = stub_ollama
    stub.always_slow = True

    async def scenario():
        cfg = dataclasses.replace(load_config(), sqlite_path=tmp_path / "t.sqlite3", ollama_base_url=url,
                                  ollama_model="stub-model", ollama_timeout_s=5, sim_speed=20)
        app = App(cfg, "llm", seed=4)
        await app.ollama.check()
        sim = asyncio.create_task(app.sim_loop())
        async with serve(app.ws_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            while not app.world.bees[0].pending:
                await asyncio.sleep(0.05)
            t0 = app.world.time
            async with connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send(json.dumps({"cmd": "snapshot"}))
                started = asyncio.get_running_loop().time()
                while json.loads(await ws.recv())["type"] != "ack":
                    pass
                assert asyncio.get_running_loop().time() - started < 0.5  # réponse immédiate malgré l'appel LLM
            assert app.world.bees[0].pending and app.world.time > t0  # la simulation avance pendant l'attente
            while app.world.bees[0].pending:
                await asyncio.sleep(0.05)
        sim.cancel()
        decision = app.world.bees[0].last_decision
        assert decision["source"] == "llm" and decision["info"]["tokens_available"] == cfg.ollama_num_ctx - 300
        row = app.db.conn.execute("SELECT source, latency_ms, tokens_available FROM decisions "
                                  "WHERE agent_id=0 AND source='llm'").fetchone()
        assert row["latency_ms"] >= 1400 and row["tokens_available"] == cfg.ollama_num_ctx - 300

    asyncio.run(scenario())


@pytest.mark.skipif(not kafka_up(), reason="Kafka n'est pas démarré (docker compose up -d)")
def test_kafka_roundtrip_sqlite_and_dedup(tmp_path):
    async def scenario():
        cfg = load_config()
        app = make_app(tmp_path, kafka_group=f"test-{uuid.uuid4().hex[:8]}")
        world = World(11)
        for _ in range(1500):
            world.step(0.1)
        world.emit(None, "run_ended", {"reason": "test"})
        events = world.drain()[0]
        producer = AIOKafkaProducer(bootstrap_servers=cfg.kafka_bootstrap, acks="all")
        await producer.start()
        try:
            for e in events + events[5:8]:  # 3 événements publiés deux fois (rejeu)
                await producer.send_and_wait(cfg.kafka_topic, json.dumps(e).encode(), key=e["run_id"].encode())
        finally:
            await producer.stop()
        consumer = AIOKafkaConsumer(cfg.kafka_topic, bootstrap_servers=cfg.kafka_bootstrap,
                                    group_id=app.cfg.kafka_group, auto_offset_reset="earliest",
                                    enable_auto_commit=False)
        await consumer.start()
        received = 0
        try:
            while received < len(events) + 3:
                batch = await asyncio.wait_for(consumer.getmany(timeout_ms=1000), 30)
                for msgs in batch.values():
                    for msg in msgs:
                        e = json.loads(msg.value)
                        if e["run_id"] == world.run_id:
                            app.consume(e, msg.partition, msg.offset)
                            received += 1
        finally:
            await consumer.stop()
        app.db.commit()
        n = app.db.conn.execute("SELECT COUNT(*) FROM events WHERE run_id=?", (world.run_id,)).fetchone()[0]
        assert n == len(events)
        m = app.metrics[world.run_id].to_dict()
        assert m["events_applied"] == len(events)
        assert m["supports"] == {s: sum(b.preference == s for b in world.bees) for s in "ABC"}
        row = app.db.conn.execute("SELECT ended_reason, final_metrics FROM runs WHERE run_id=?",
                                  (world.run_id,)).fetchone()
        assert row["ended_reason"] == "test" and json.loads(row["final_metrics"])["supports"] == m["supports"]
        # après un « redémarrage » (nouvel App, même base), les indicateurs se reconstruisent depuis SQLite
        app2 = App(app.cfg, "regles", seed=3)
        assert app2.consume(events[-1]) is False  # doublon détecté
        assert app2.metrics[world.run_id].to_dict()["supports"] == m["supports"]

    asyncio.run(scenario())
