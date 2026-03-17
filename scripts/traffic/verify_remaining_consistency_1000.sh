#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LINE_COUNT=1000 "$SCRIPT_DIR/verify_remaining_consistency_scaled.sh" "$@"
