#!/usr/bin/env bash
# Vérification complète : outils, Kafka, tests Python (dont Kafka->SQLite), projet Godot, WebSocket Godot, OpenJev.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
GODOT="${GODOT:-godot}"
FAIL=0
ok()   { echo "  ✓ $*"; }
ko()   { echo "  ✗ $*"; FAIL=1; }
warn() { echo "  ⚠ $*"; }

echo "1. Outils"
[ -x "$PY" ] && ok "$($PY --version) (venv .venv)" || { ko "venv absent : lancez scripts/setup.sh"; exit 1; }
docker compose version >/dev/null 2>&1 && ok "$(docker compose version)" || ko "Docker Compose introuvable"
command -v "$GODOT" >/dev/null && ok "Godot $("$GODOT" --version 2>/dev/null | tail -1)" || ko "Godot introuvable (variable GODOT)"
command -v ollama >/dev/null && ok "$(ollama --version 2>/dev/null | tail -1)" || warn "Ollama introuvable"

echo "2. Kafka (KRaft, Docker)"
if docker compose up -d --wait kafka >/dev/null 2>&1 && docker compose up kafka-init >/dev/null 2>&1; then
  ok "broker sain : $(docker inspect -f '{{.State.Health.Status}}' bee-kafka)"
  TOPIC=$(docker exec bee-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic bee.events.v1 2>&1)
  [[ "$TOPIC" == *"PartitionCount: 3"* ]] && ok "topic bee.events.v1 présent (3 partitions)" || ko "topic bee.events.v1 absent"
else
  ko "Kafka ne démarre pas (docker compose logs kafka)"
fi

echo "3. Tests Python (simulation, indicateurs, SQLite, WebSocket, Kafka aller-retour)"
if (cd backend && ../$PY -m pytest -q -rs 2>&1 | tail -4 | sed 's/^/    /'; exit "${PIPESTATUS[0]}"); then ok "pytest"; else ko "pytest"; fi

echo "4. Projet Godot"
"$GODOT" --headless --path godot --import >/dev/null 2>&1
ERR=$("$GODOT" --headless --path godot --check-only --script res://main.gd 2>&1 | grep -E "SCRIPT ERROR|Parse Error" || true)
[ -z "$ERR" ] && ok "import + analyse de main.gd sans erreur" || ko "main.gd : $ERR"

echo "5. WebSocket Python ↔ Godot (serveur temporaire sur le port 8799)"
TMPDB=$(mktemp -d)/verify.sqlite3
PYTHONPATH=backend WS_PORT=8799 SQLITE_PATH="$TMPDB" SIM_SPEED=10 KAFKA_GROUP=verify-$$ \
  $PY -m essaim.server --mode regles >/dev/null 2>&1 &
SERVER=$!
sleep 4
OUT=$(ESSAIM_WS_URL=ws://127.0.0.1:8799 "$GODOT" --headless --path godot --script res://tests/ws_smoke.gd 2>&1)
echo "$OUT" | grep -q GODOT_WS_OK && ok "$(echo "$OUT" | grep 'snapshot reçu')" || ko "Godot n'a pas reçu de snapshot"
for _ in $(seq 20); do  # un nouveau groupe consommateur met quelques secondes à rejoindre le broker
  N=$(sqlite3 "$TMPDB" "select count(*) from events" 2>/dev/null || echo 0)
  [ "${N:-0}" -gt 0 ] && break
  sleep 1
done
[ "${N:-0}" -gt 0 ] && ok "$N événements consommés depuis Kafka et sauvegardés dans SQLite" || ko "aucun événement en SQLite"
kill $SERVER 2>/dev/null; wait $SERVER 2>/dev/null

echo "6. Ollama / OpenJev"
scripts/check_openjev.sh | sed 's/^/    /'
case "${PIPESTATUS[0]}" in
  0) (cd backend && ../$PY -m essaim.check_llm | tail -12 | sed 's/^/    /'; exit "${PIPESTATUS[0]}") \
       && ok "appel réel OpenJev valide" || ko "appel réel OpenJev en échec" ;;
  2) warn "OpenJev non testé : ressources insuffisantes (blocage réel, voir ci-dessus)" ;;
  *) warn "OpenJev non testé : Ollama indisponible" ;;
esac

echo
[ $FAIL -eq 0 ] && echo "VÉRIFICATION OK" || echo "VÉRIFICATION EN ÉCHEC"
exit $FAIL
