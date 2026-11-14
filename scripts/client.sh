#!/usr/bin/env bash
# Example: scripts/client.sh --target localhost:9001 put name Tarini
exec java -jar "$(dirname "$0")/../client/target/client.jar" "$@"
