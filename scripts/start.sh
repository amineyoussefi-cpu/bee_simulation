#!/usr/bin/env bash
# Démarre Kafka (Docker) puis le serveur Python. Arguments transmis au serveur : --mode llm|regles --seed N
set -euo pipefail
cd "$(dirname "$0")/.."
[ -x .venv/bin/python ] || scripts/setup.sh
echo "• Kafka (KRaft) : démarrage et attente du contrôle de santé…"
docker compose up -d --wait kafka
docker compose up kafka-init >/dev/null   # crée bee.events.v1 s'il n'existe pas (idempotent)
echo "• Serveur Python (Ctrl+C pour arrêter). Lancez ensuite Godot : scripts/godot.sh"
cd backend && exec ../.venv/bin/python -m essaim.server "$@"
