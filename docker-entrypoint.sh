#!/bin/sh
# Entrypoint do container CONNECTONGS
# Decide o papel do container (api ou worker) via variável ROLE
set -e

if [ "$ROLE" = "api" ]; then
    exec python -m connectongs.api_server
else
    exec python worker_client.py "$@"
fi
