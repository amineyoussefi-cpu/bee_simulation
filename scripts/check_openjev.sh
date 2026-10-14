#!/usr/bin/env bash
# Vérifie si la machine peut accueillir OpenJev AVANT tout téléchargement (~16,5 Go, 32 Go de RAM conseillés sur Mac).
#   scripts/check_openjev.sh          -> diagnostic seul
#   scripts/check_openjev.sh --pull   -> télécharge seulement si les ressources suffisent
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && . ./.env; set +a
MODEL="${OLLAMA_MODEL:-hf.co/openjev/openjev-GGUF:Q4_K_M}"
URL="${OLLAMA_BASE_URL:-http://localhost:11434}"
RAM_GB=$(( $(sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/{print $2*1024}' /proc/meminfo) / 1073741824 ))
FREE_GB=$(df -g . 2>/dev/null | awk 'NR==2{print $4}' || echo "?")
echo "Modèle demandé : $MODEL"
echo "RAM : ${RAM_GB} Go (32 Go requis) · disque libre : ${FREE_GB} Go (~17 Go requis)"
if ! curl -sf -m 5 "$URL/api/tags" >/dev/null; then echo "BLOCAGE : Ollama ne répond pas sur $URL (lancez l'application Ollama)."; exit 1; fi
if curl -sf -m 5 "$URL/api/tags" | grep -q "\"$MODEL\""; then echo "OK : modèle déjà présent dans Ollama."; exit 0; fi
echo "Modèle absent localement."
curl -sf -m 15 https://huggingface.co/api/models/openjev/openjev-GGUF | grep -q "OpenJev-Q4_K_M.gguf" \
  && echo "Disponible sur Hugging Face (OpenJev-Q4_K_M.gguf)." || echo "Impossible de confirmer la disponibilité sur Hugging Face."
if [ "$RAM_GB" -lt 32 ]; then
  echo "BLOCAGE : ${RAM_GB} Go de RAM < 32 Go requis. Téléchargement non effectué ; la simulation tourne"
  echo "          avec l'abeille 0 en repli à règles (décisions marquées 'fallback'). Aucun modèle de remplacement."
  exit 2
fi
if [ "${1:-}" = "--pull" ]; then ollama pull "$MODEL"; else echo "Ressources suffisantes : relancez avec --pull pour télécharger."; fi
