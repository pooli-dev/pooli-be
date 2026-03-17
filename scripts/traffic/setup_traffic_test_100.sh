#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"$SCRIPT_DIR/setup_traffic_with_cache_reset.sh" "$SCRIPT_DIR/setup_traffic_test_100.sql"
