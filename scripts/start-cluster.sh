#!/usr/bin/env bash
# Starts 5 nodes in the background (ports 9001 to 9005). Logs go to logs/nodeN.log.
# Stop everything with: pkill -f node.jar     Crash one node with: pkill -f "id node3"
# Run "mvn clean package" first.
set -e
cd "$(dirname "$0")/.."
JAR=node/target/node.jar
PEERS=localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005

[ -f "$JAR" ] || { echo "node.jar not found. Run 'mvn clean package' first."; exit 1; }
mkdir -p logs
for i in 1 2 3 4 5; do
  java -jar "$JAR" --id "node$i" --port "900$i" --peers "$PEERS" > "logs/node$i.log" 2>&1 &
done
echo "Started 5 nodes. Logs are in logs/"
