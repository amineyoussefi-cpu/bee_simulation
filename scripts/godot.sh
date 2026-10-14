#!/usr/bin/env bash
# Ouvre la visualisation Godot 4 (le serveur Python doit tourner).
set -euo pipefail
cd "$(dirname "$0")/.."
GODOT="${GODOT:-godot}"
"$GODOT" --path godot --import >/dev/null 2>&1 || true
exec "$GODOT" --path godot "$@"
