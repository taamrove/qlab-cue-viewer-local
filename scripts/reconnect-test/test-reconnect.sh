#!/bin/zsh
# Does the bridge's RelayPublisher recover when the relay dies?
#
#   scripts/reconnect-test/test-reconnect.sh [path/to/relay-server] [publisher.swift]
#
# Scenario: connect, then FREEZE the relay (SIGSTOP: the TCP connection stays
# open but nothing answers — the half-open case a reverse proxy can leave
# behind), then kill it and start a fresh one (a redeploy). A healthy
# publisher logs "disconnected" during the freeze and "CONNECTED" again after
# the restart.
set -u
HERE=${0:A:h}
RELAY=${1:-$HOME/Dev/relay-server}
PUB=${2:-$HERE/../../Sources/QlabCueViewer/RelayPublisher.swift}
PORT=18771
WORK=$(mktemp -d)
trap 'kill -9 $RPID 2>/dev/null; kill $HPID 2>/dev/null; rm -rf $WORK' EXIT

swiftc -swift-version 5 -O -o $WORK/harness $PUB $HERE/main.swift 2>&1 | grep -v warning || true
[ -x $WORK/harness ] || { echo "build failed"; exit 1; }

start_relay() { (cd $RELAY && PORT=$PORT RELAY_TOKEN=harness-token DATA_DIR=$WORK/data node src/server.js >>$WORK/relay.log 2>&1 &); sleep 0.5; RPID=$(lsof -ti tcp:$PORT -sTCP:LISTEN | head -1); }
publishers() { curl -s localhost:$PORT/health | grep -o '"publishers":[0-9]*' | head -1; }
mark() { echo "------  $1"; }

start_relay
$WORK/harness ws://127.0.0.1:$PORT 45 & HPID=$!
sleep 4;  mark "connected? $(publishers)"
mark "freeze relay (half-open)"; kill -STOP $RPID
sleep 14
mark "kill frozen relay, start a fresh one (redeploy)"; kill -9 $RPID; sleep 0.5; start_relay
sleep 16; mark "after restart: $(publishers)"
kill $HPID 2>/dev/null; wait $HPID 2>/dev/null
