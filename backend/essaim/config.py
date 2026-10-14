"""Configuration lue depuis l'environnement (et le fichier .env à la racine s'il existe)."""
import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class Config:
    seed: int
    mode: str  # "llm" (abeille 0 pilotée par Ollama) ou "regles"
    sim_dt: float
    sim_speed: float
    sim_max_time: float
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group: str
    ollama_base_url: str
    ollama_model: str
    ollama_timeout_s: float
    ollama_num_ctx: int
    ws_host: str
    ws_port: int
    sqlite_path: Path
    consensus_min_bees: int
    consensus_hold_s: float


def load_config() -> Config:
    _load_dotenv(ROOT / ".env")
    env = os.environ.get
    sqlite_path = Path(env("SQLITE_PATH", "data/essaim.sqlite3"))
    return Config(
        seed=int(env("SIM_SEED", "42")),
        mode=env("SIM_MODE", "llm"),
        sim_dt=float(env("SIM_DT", "0.1")),
        sim_speed=float(env("SIM_SPEED", "4")),
        sim_max_time=float(env("SIM_MAX_TIME", "600")),
        kafka_bootstrap=env("KAFKA_BOOTSTRAP", "localhost:9092"),
        kafka_topic=env("KAFKA_TOPIC", "bee.events.v1"),
        kafka_group=env("KAFKA_GROUP", "bee-metrics"),
        ollama_base_url=env("OLLAMA_BASE_URL", "http://localhost:11434"),
        ollama_model=env("OLLAMA_MODEL", "hf.co/openjev/openjev-GGUF:Q4_K_M"),
        ollama_timeout_s=float(env("OLLAMA_TIMEOUT_S", "30")),
        ollama_num_ctx=int(env("OLLAMA_NUM_CTX", "4096")),
        ws_host=env("WS_HOST", "127.0.0.1"),
        ws_port=int(env("WS_PORT", "8765")),
        sqlite_path=sqlite_path if sqlite_path.is_absolute() else ROOT / sqlite_path,
        consensus_min_bees=int(env("CONSENSUS_MIN_BEES", "8")),
        consensus_hold_s=float(env("CONSENSUS_HOLD_S", "10")),
    )
