#!/bin/bash

# ── Colors ─────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

DISPATCHER_URL="http://localhost:8000"
API_KEY="client-secret"
LOGFILE="/tmp/rideshare_all.log"
LOG_PID=""

header()  { echo ""; echo -e "${CYAN}══════════════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}══════════════════════════════════════════════════${NC}"; }
info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[DEMO]${NC} $1"; }
log()     { echo -e "${RED}[LOG]${NC}  $1"; }

submit_ride() {
  curl -s -X POST "$DISPATCHER_URL/rides" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d '{"pickup": "Airport", "dropoff": "Hotel"}'
}

get_id()     { echo "$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ride_id','?'))" 2>/dev/null; }
get_status() { echo "$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','error'))" 2>/dev/null; }

redis_cmd() { docker compose exec -T redis redis-cli "$@" 2>/dev/null > /dev/null; }
redis_out() { docker compose exec -T redis redis-cli "$@" 2>/dev/null | tr -d '\r'; }

# ── Global continuous logger — start once, runs for entire demo ────────────────
start_global_logging() {
  > "$LOGFILE"
  docker compose logs -f --no-log-prefix dispatcher1 dispatcher2 watchdog 2>/dev/null >> "$LOGFILE" &
  LOG_PID=$!
  sleep 1
  info "Continuous log capture started → $LOGFILE"
}

stop_global_logging() {
  [ -n "$LOG_PID" ] && kill $LOG_PID 2>/dev/null && wait $LOG_PID 2>/dev/null
  LOG_PID=""
}

# ── Show logs between two timestamps, with optional pattern and ride_id filter ─
# Usage: show_logs_between "2026-03-29 22:10:00" "2026-03-29 22:11:30" "pattern" count "ride_id"
show_logs_between() {
  local ts_start="$1"
  local ts_end="$2"
  local pattern="$3"
  local count="${4:-15}"
  local ride_id="${5:-}"
  local matches

  if [ -n "$ride_id" ]; then
    matches=$(grep -E "$pattern" "$LOGFILE" \
      | grep "$ride_id" \
      | awk -v s="$ts_start" -v e="$ts_end" -F'"asctime": "' \
          'NF>1 { split($2,a,"\""); if(a[1] >= s && a[1] <= e) print }' \
      | tail -"$count")
  else
    matches=$(grep -E "$pattern" "$LOGFILE" \
      | awk -v s="$ts_start" -v e="$ts_end" -F'"asctime": "' \
          'NF>1 { split($2,a,"\""); if(a[1] >= s && a[1] <= e) print }' \
      | tail -"$count")
  fi

  if [ -n "$matches" ]; then
    echo "$matches" | while IFS= read -r line; do log "$line"; done
  else
    echo -e "${YELLOW}  (no matching log entries in this time window)${NC}"
  fi
}

now() { date -u +"%Y-%m-%d %H:%M:%S"; }

# ══════════════════════════════════════════════════
# SCENARIO 1 — Dispatcher Crash + Auto Recovery
# ══════════════════════════════════════════════════
demo_dispatcher_crash() {
  header "SCENARIO 1: Dispatcher Crash + Auto Recovery"
  local T_START; T_START=$(now)

  info "Submitting 2 rides while BOTH dispatchers are healthy..."
  for i in 1 2; do
    r=$(submit_ride); id=$(get_id "$r")
    success "Ride submitted: $id"
  done
  sleep 3

  warn "Killing dispatcher1..."
  docker compose stop dispatcher1 2>/dev/null
  success "dispatcher1 is DOWN"

  info "Submitting 3 rides — Nginx must route ALL to dispatcher2..."
  for i in 1 2 3; do
    r=$(submit_ride); id=$(get_id "$r"); status=$(get_status "$r")
    [ "$status" == "queued" ] \
      && success "Ride $id → dispatcher2 ✓" \
      || echo -e "${RED}[FAIL]${NC} $r"
  done
  sleep 3

  warn "Restarting dispatcher1..."
  docker compose start dispatcher1 2>/dev/null
  sleep 5

  info "Submitting 1 final ride to confirm recovery..."
  r=$(submit_ride); id=$(get_id "$r"); status=$(get_status "$r")
  [ "$status" == "queued" ] \
    && success "Recovery ride $id queued ✓" \
    || echo -e "${RED}[FAIL]${NC}"
  sleep 2

  local T_END; T_END=$(now)
  echo ""
  info "Log evidence [$T_START → $T_END]"
  info "  First 2 rides: both dispatchers active. During outage: dispatcher-2 only."
  info "  After recovery: dispatcher-1 rejoins and picks rides from shared queue."
  show_logs_between "$T_START" "$T_END" "ride submitted|assigned successfully" 16
}

# ══════════════════════════════════════════════════
# SCENARIO 2 — Driver Goes Offline
# ══════════════════════════════════════════════════
demo_driver_offline() {
  header "SCENARIO 2: Driver Goes Offline"

  info "Ensuring driver1 is up and registered..."
  docker compose start driver1 2>/dev/null
  sleep 4

  local T_START; T_START=$(now)

  warn "Killing driver1..."
  docker compose stop driver1 2>/dev/null
  success "driver1 is DOWN"
  sleep 1

  info "Pushing driver1 to front of queue — dispatcher tries it first and gets ConnectError..."
  redis_cmd LPUSH driver:queue driver-1
  success "driver1 at front of queue"

  info "Submitting ride..."
  r=$(submit_ride); RIDE_ID=$(get_id "$r")
  success "Submitted ride: $RIDE_ID"
  info "Expected: dispatcher tries driver1 → ConnectError → skips → assigns to another driver"

  sleep 8

  local T_END; T_END=$(now)
  echo ""
  info "Log evidence for ride $RIDE_ID [$T_START → $T_END]:"
  info "  - unreachable = ConnectError on driver1 (offline)"
  info "  - assigned successfully = dispatcher retried with next driver"
  show_logs_between "$T_START" "$T_END" "unreachable|offline|assigned successfully" 6 "$RIDE_ID"
  echo ""
  info "All ConnectError/unreachable events in this window:"
  show_logs_between "$T_START" "$T_END" "unreachable|offline" 4

  warn "Restarting driver1..."
  docker compose start driver1 2>/dev/null
  sleep 4
  success "driver1 is back UP ✓"
}

# ══════════════════════════════════════════════════
# SCENARIO 3 — Driver Timeout
# ══════════════════════════════════════════════════
demo_driver_timeout() {
  header "SCENARIO 3: Driver Timeout"

  info "Ensuring driver2 is up..."
  docker compose start driver2 2>/dev/null
  sleep 4

  info "Triggering slow mode on driver2 (will sleep 10s, dispatcher timeout = 5s)..."
  curl -s -X POST "http://localhost:9002/simulate/slow" > /dev/null
  success "driver2 will timeout on next assignment"

  info "Pushing driver2 to front of queue so it gets picked first..."
  redis_cmd LPUSH driver:queue driver-2
  sleep 0.5

  local T_START; T_START=$(now)

  info "Submitting ride..."
  r=$(submit_ride); RIDE_ID=$(get_id "$r")
  success "Submitted ride: $RIDE_ID"
  info "Expected: dispatcher picks driver2 → 5s timeout → retries with next driver"

  info "Waiting 12s for timeout + retry to complete..."
  sleep 12

  local T_END; T_END=$(now)
  echo ""
  info "Log evidence for ride $RIDE_ID [$T_START → $T_END]:"
  info "  - driver timed out = driver2 exceeded 5s timeout"
  info "  - assigned successfully = dispatcher retried (slow mode fires once only)"
  show_logs_between "$T_START" "$T_END" "timed out|assigned successfully" 6 "$RIDE_ID"
}

# ══════════════════════════════════════════════════
# SCENARIO 4 — Security
# ══════════════════════════════════════════════════
demo_security() {
  header "SCENARIO 4: Security — Auth + Input Validation"

  info "Test 1: No API key → expect 401..."
  r=$(curl -s -X POST "$DISPATCHER_URL/rides" -H "Content-Type: application/json" -d '{"pickup":"A","dropoff":"B"}')
  log "Response: $r"
  echo "$r" | grep -q "Invalid API key" && success "401 Unauthorized ✓" || echo -e "${RED}[FAIL]${NC}"

  echo ""
  info "Test 2: Wrong API key → expect 401..."
  r=$(curl -s -X POST "$DISPATCHER_URL/rides" -H "Content-Type: application/json" -H "X-API-Key: wrongkey" -d '{"pickup":"A","dropoff":"B"}')
  log "Response: $r"
  echo "$r" | grep -q "Invalid API key" && success "401 Unauthorized ✓" || echo -e "${RED}[FAIL]${NC}"

  echo ""
  info "Test 3: Valid key + malformed body → expect 422..."
  r=$(curl -s -X POST "$DISPATCHER_URL/rides" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{"bad":"data"}')
  log "Response: $r"
  echo "$r" | grep -q "missing" && success "422 Unprocessable Entity ✓" || echo -e "${RED}[FAIL]${NC}"

  echo ""
  info "Test 4: Valid key + valid body → expect 202..."
  r=$(submit_ride); id=$(get_id "$r")
  log "Response: $r"
  echo "$r" | grep -q "queued" && success "202 Accepted — ride $id queued ✓" || echo -e "${RED}[FAIL]${NC}"
}

# ══════════════════════════════════════════════════
# SCENARIO 5 — Queue Overload
# ══════════════════════════════════════════════════
demo_overload() {
  header "SCENARIO 5: Queue Overload"

  warn "Stopping ALL dispatchers and drivers..."
  docker compose stop dispatcher1 dispatcher2 driver1 driver2 driver3 driver4 driver5 2>/dev/null
  sleep 2
  success "All dispatchers and drivers DOWN"

  info "Pushing 20 rides into ride:queue..."
  for i in $(seq 1 20); do
    redis_cmd RPUSH ride:queue \
      "{\"ride_id\":\"overload-$i\",\"pickup\":\"Location $i\",\"dropoff\":\"Dest $i\",\"submitted_at\":\"now\",\"client_ip\":\"demo\"}"
  done
  success "20 rides injected"

  depth=$(redis_out LLEN ride:queue)
  log "ride:queue length = $depth"
  [ "$depth" -gt 0 ] \
    && success "Queue is full ✓  (check RedisInsight at http://localhost:5540)" \
    || warn "Queue unexpectedly empty"

  warn "Starting drivers first, waiting for registration..."
  docker compose start driver1 driver2 driver3 driver4 driver5 2>/dev/null
  sleep 6
  success "Drivers registered"

  local T_START; T_START=$(now)

  warn "Starting dispatchers — queue will now drain..."
  docker compose start dispatcher1 dispatcher2 2>/dev/null

  info "Waiting 12s for queue to drain..."
  sleep 12

  depth=$(redis_out LLEN ride:queue)
  log "ride:queue length after drain = $depth"
  [ "$depth" -eq 0 ] \
    && success "Queue fully drained ✓" \
    || warn "Queue still has $depth rides"

  local T_END; T_END=$(now)
  echo ""
  info "Log evidence [$T_START → $T_END] — dispatchers draining overload-* rides:"
  show_logs_between "$T_START" "$T_END" "overload-|no drivers|re-queued|assigned successfully" 20
}

# ══════════════════════════════════════════════════
# SCENARIO 6 — Watchdog Rescue
# ══════════════════════════════════════════════════
demo_watchdog() {
  header "SCENARIO 6: Watchdog Rescue"

  local T_START; T_START=$(now)

  info "Injecting driver-99 into pending_assignments with timestamp 20s in the past..."
  STALE_TIME=$(python3 -c "import time; print(int(time.time()) - 20)")
  redis_cmd ZADD pending_assignments "$STALE_TIME" driver-99
  redis_cmd SET "driver:driver-99:url" "http://driver99:8000"
  success "driver-99 injected as stale (20s old, threshold = 15s)"

  info "Waiting 10s for watchdog scan cycle to fire..."
  for i in $(seq 10 -1 1); do
    printf "\r  ${YELLOW}Watchdog scanning in %2d seconds...${NC}  " "$i"
    sleep 1
  done
  echo ""

  local T_END; T_END=$(now)
  echo ""
  info "Log evidence [$T_START → $T_END] — watchdog rescued driver-99:"
  show_logs_between "$T_START" "$T_END" "rescued|stale" 4

  redis_cmd DEL "driver:driver-99:url"
  redis_cmd ZREM pending_assignments driver-99
}

# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════
# echo ""
# echo -e "${CYAN}  Distributed Ride-Request Platform — Failure Demo Script${NC}"
# echo -e "${CYAN}  ─────────────────────────────────────────────────────────${NC}"
# echo ""
# echo "  1) Dispatcher crash + auto recovery"
# echo "  2) Driver goes offline"
# echo "  3) Driver timeout"
# echo "  4) Security (auth + input validation)"
# echo "  5) Queue overload"
# echo "  6) Watchdog rescue"
# echo "  7) Run ALL scenarios"
# echo ""
# read -p "  Choose scenario [1-7]: " choice

# Start single continuous logger before any scenario runs
start_global_logging

demo_dispatcher_crash; sleep 2
demo_driver_offline;   sleep 2
demo_driver_timeout;   sleep 2
demo_security;         sleep 2
demo_overload;         sleep 2
demo_watchdog

stop_global_logging

echo ""
echo -e "${GREEN}  Demo complete. Full logs saved to: $LOGFILE${NC}"
echo ""