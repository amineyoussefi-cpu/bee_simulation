"""Persistance SQLite : runs, événements (sans doublons) et décisions."""
import json
import sqlite3
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    seed INTEGER, mode TEXT, model TEXT,
    started_sim_at REAL, started_wall_at REAL,
    ended_reason TEXT, ended_sim_at REAL,
    final_metrics TEXT
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    sim_time REAL NOT NULL,
    agent_id INTEGER,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    kafka_partition INTEGER, kafka_offset INTEGER,
    stored_at REAL NOT NULL,
    UNIQUE (run_id, sequence)
);
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    decision_id TEXT,
    agent_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    requested_sim_time REAL,
    applied_sim_time REAL NOT NULL,
    options TEXT NOT NULL,
    choice TEXT,
    action TEXT,
    source TEXT NOT NULL,          -- règles | llm | fallback | ignorée
    latency_ms REAL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    context_tokens INTEGER,
    tokens_available INTEGER,
    raw_response TEXT,
    error TEXT,
    stored_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_run ON events(run_id, sequence);
"""


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def insert_event(self, e: dict, partition: int | None = None, offset: int | None = None) -> bool:
        """Insère l'événement ; retourne False si déjà présent (event_id ou (run_id, sequence))."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (e["event_id"], e["run_id"], e["sequence"], e["sim_time"], e["agent_id"], e["event_type"],
             e["schema_version"], json.dumps(e["payload"], ensure_ascii=False), partition, offset, time.time()))
        if cur.rowcount and e["event_type"] == "run_started":
            p = e["payload"]
            self.conn.execute(
                "INSERT OR IGNORE INTO runs (run_id, seed, mode, model, started_sim_at, started_wall_at) "
                "VALUES (?,?,?,?,?,?)",
                (e["run_id"], p["seed"], p.get("mode"), p.get("model"), e["sim_time"], time.time()))
        return cur.rowcount == 1

    def end_run(self, run_id: str, reason: str, sim_time: float, metrics: dict) -> None:
        self.conn.execute("UPDATE runs SET ended_reason=?, ended_sim_at=?, final_metrics=? WHERE run_id=?",
                          (reason, sim_time, json.dumps(metrics, ensure_ascii=False), run_id))

    def insert_decision(self, d: dict) -> None:
        self.conn.execute(
            "INSERT INTO decisions (run_id, decision_id, agent_id, kind, requested_sim_time, applied_sim_time, "
            "options, choice, action, source, latency_ms, prompt_tokens, completion_tokens, context_tokens, "
            "tokens_available, raw_response, error, stored_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (d["run_id"], d.get("decision_id"), d["agent_id"], d["kind"], d.get("requested_at"), d["sim_time"],
             json.dumps(d["options"], ensure_ascii=False), d.get("choice"), d.get("action"), d["source"],
             d.get("latency_ms"), d.get("prompt_tokens"), d.get("completion_tokens"), d.get("context_tokens"),
             d.get("tokens_available"), d.get("raw_response"), d.get("error") or d.get("fallback_reason"),
             time.time()))

    def run_events(self, run_id: str) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM events WHERE run_id=? ORDER BY sequence", (run_id,)).fetchall()
        return [{"event_id": r["event_id"], "run_id": r["run_id"], "sequence": r["sequence"],
                 "sim_time": r["sim_time"], "agent_id": r["agent_id"], "event_type": r["event_type"],
                 "schema_version": r["schema_version"], "payload": json.loads(r["payload"])} for r in rows]

    def commit(self) -> None:
        self.conn.commit()
