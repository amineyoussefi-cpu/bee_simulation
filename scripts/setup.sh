#!/usr/bin/env bash
# Prépare l'environnement Python (3.12, dépendances épinglées) et le fichier .env.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .env ]; then cp .env.example .env && echo "• .env créé depuis .env.example"; fi
if command -v uv >/dev/null; then
  uv venv --python 3.12 .venv
  uv pip install --python .venv/bin/python -r backend/requirements.txt
else
  python3.12 -m venv .venv
  .venv/bin/pip install -r backend/requirements.txt
fi
echo "• Python : $(.venv/bin/python --version)"
