# Distributed Ride-Request Simulation Platform
ENGR 5710G — Network Computing (Winter 2026)

## Prerequisites
- Docker Desktop
- Python 3.11+

---

## Setup & Run

### 1. Clone and start all services
```bash
docker compose up --build
```

Wait ~10 seconds for drivers to register, then verify:
```bash
curl http://localhost:8001/health
# available_drivers should be 3 or more
```

### 2. Run the client simulator
```bash
cd client
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py --rides 20 --concurrency 5
```

### 3. Run Locust load testing (optional)
```bash
cd client
source venv/bin/activate
pip install locust
locust -f locustfile.py --host=http://localhost:8000
```
Open `http://localhost:8089` in browser.

---

## Services & Ports

| Service       | URL                          | Description                  |
|---------------|------------------------------|------------------------------|
| Nginx (LB)    | http://localhost:8000        | Load balancer entry point    |
| Dispatcher 1  | http://localhost:8001        | Ride dispatcher instance 1   |
| Dispatcher 2  | http://localhost:8002        | Ride dispatcher instance 2   |
| Driver 1–5    | http://localhost:9001–9005   | Driver nodes                 |
| Grafana       | http://localhost:3000        | Metrics dashboard (admin/admin) |
| Prometheus    | http://localhost:9090        | Raw metrics                  |
| RedisInsight  | http://localhost:5540        | Redis queue inspector        |
| Redis         | localhost:6380               | Message broker & state store |

---

## Failure Scenario Demos

### 1. Dispatcher crash + automatic recovery
```bash
# Kill one dispatcher — Nginx auto-routes to the other, zero failures
docker compose stop dispatcher1

# Bring it back
docker compose start dispatcher1
```

### 2. Driver offline
```bash
docker compose stop driver1
# Dispatcher catches ConnectError, skips to next driver
docker compose start driver1
```

### 3. Driver timeout
```bash
curl -X POST http://localhost:9002/simulate/slow
# Next ride assigned to that driver triggers a 5s timeout
# Dispatcher logs: "driver timed out", retries with next driver
```

### 4. Queue overload (no drivers)
```bash
docker compose stop driver1 driver2 driver3 driver4 driver5
curl -X POST "http://localhost:8000/simulate/overload?count=20"
# Watch ride:queue fill up in RedisInsight
docker compose start driver1 driver2 driver3 driver4 driver5
# Watch queue drain as rides get assigned
```

### 5. Rate limiting
```bash
# Set RATE_LIMIT_REQUESTS=10 in docker-compose.yml, rebuild
# Run Locust with high concurrency — 429s appear after limit hit
```

### 6. Watchdog rescue (stale assignment)
```bash
# Kill a dispatcher mid-assignment
docker compose stop dispatcher1
# Watchdog detects driver stuck in pending_assignments after 15s
docker compose logs -f watchdog
# Logs show: "rescued stale driver — pushed back to queue"
```

---

## Grafana Setup
1. Open http://localhost:3000 (admin/admin)
2. Connections → Data sources → Add → Prometheus
3. URL: `http://prometheus:9090` → Save & test
4. Dashboards → New → Add visualization → query `http_requests_total`

## RedisInsight Setup
1. Open http://localhost:5540
2. Add database: Host `redis`, Port `6379`
3. Browse tab shows all queues and keys live

---

## Project Structure
```
/
├── docker-compose.yml
├── nginx.conf
├── prometheus.yml
├── dispatcher/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── driver/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── watchdog/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
└── client/
    ├── main.py
    ├── locustfile.py
    └── requirements.txt
```

---

## Environment Variables

| Variable               | Default      | Description                          |
|------------------------|--------------|--------------------------------------|
| REDIS_URL              | redis://...  | Redis connection string              |
| DISPATCHER_ID          | dispatcher-1 | Unique dispatcher identifier         |
| DRIVER_SECRET          | supersecret  | Shared auth token for driver reg     |
| RATE_LIMIT_REQUESTS    | 1000         | Max requests per IP per window       |
| RATE_LIMIT_WINDOW      | 60           | Rate limit window in seconds         |
| ASSIGN_TIMEOUT         | 5.0          | Driver HTTP timeout in seconds       |
| MAX_RETRIES            | 3            | Max driver assignment attempts       |
| REJECTION_RATE         | 0.2          | Driver rejection probability (0–1)   |
| STALE_THRESHOLD_SECONDS| 15           | Watchdog stale assignment cutoff     |