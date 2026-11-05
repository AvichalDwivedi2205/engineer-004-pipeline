#!/bin/sh
# Runs schema/clickhouse.sql plus schema/check.sql in clickhouse-local (no server, no network).
# Expected: 7 rows stored, 5 with FINAL, 4 with LIMIT 1 BY, rollup 4 before repair and 2 after,
# 3 after the deletion mutation (the comments in schema/check.sql say why).
cd "$(dirname "$0")/.." || exit 1
if ! command -v clickhouse >/dev/null 2>&1; then
    echo "ddl_check: clickhouse binary not found, skipped (macOS: brew install --cask clickhouse)"
    exit 0
fi
cat schema/clickhouse.sql schema/check.sql | clickhouse local --multiquery
