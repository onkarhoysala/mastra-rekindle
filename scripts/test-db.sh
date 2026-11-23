#!/usr/bin/env bash
# Throwaway Postgres for tests, using a local install (Homebrew etc.). No Docker needed.
#   bash scripts/test-db.sh start   -> cluster in ./.pgdata on port 54329
#   bash scripts/test-db.sh stop
# Then: TEST_DATABASE_URL=postgres://postgres@localhost:54329/postgres npm test
# Any other Postgres works too: point TEST_DATABASE_URL at it.
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)/.pgdata"
PORT="${PGPORT_TEST:-54329}"

case "${1:-}" in
  start)
    if [ ! -d "$DIR" ]; then
      echo "initdb -> $DIR"
      initdb -D "$DIR" -U postgres --auth=trust >/dev/null
    fi
    echo "starting postgres on port $PORT"
    pg_ctl -D "$DIR" -o "-p $PORT -k /tmp" -l "$DIR/server.log" -w start
    echo "ready: postgres://postgres@localhost:$PORT/postgres"
    ;;
  stop)
    pg_ctl -D "$DIR" -m fast stop
    ;;
  *)
    echo "usage: $0 start|stop" >&2
    exit 1
    ;;
esac
