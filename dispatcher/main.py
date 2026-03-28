import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator

# ── Logging ────────────────────────────────────────────────────────────────────
logger = logging.getLogger("dispatcher")
_handler = logging.StreamHandler()
_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_URL             = os.getenv("REDIS_URL", "redis://localhost:6379")
DISPATCHER_ID         = os.getenv("DISPATCHER_ID", "dispatcher-1")
DRIVER_SECRET         = os.getenv("DRIVER_SECRET", "supersecret")
RATE_LIMIT_REQUESTS   = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
RATE_LIMIT_WINDOW     = int(os.getenv("RATE_LIMIT_WINDOW", "10"))   # seconds
ASSIGN_TIMEOUT        = float(os.getenv("ASSIGN_TIMEOUT", "5.0"))   # seconds
MAX_RETRIES           = int(os.getenv("MAX_RETRIES", "3"))

# ── Shared Redis client ────────────────────────────────────────────────────────
redis_client: aioredis.Redis = None


# ── Models ─────────────────────────────────────────────────────────────────────
class RideRequest(BaseModel):
    pickup: str
    dropoff: str

    @field_validator("pickup", "dropoff")
    @classmethod
    def validate_location(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("location must not be empty")
        if len(v) > 200:
            raise ValueError("location must be under 200 characters")
        return v


class DriverRegistration(BaseModel):
    driver_id: str
    url: str
    token: str


# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    loop_task = asyncio.create_task(dispatcher_loop())
    logger.info("dispatcher started", extra={"dispatcher_id": DISPATCHER_ID})
    yield
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass
    await redis_client.aclose()
    logger.info("dispatcher shut down", extra={"dispatcher_id": DISPATCHER_ID})


app = FastAPI(title=f"Dispatcher Service [{DISPATCHER_ID}]", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


# ── Rate-limiting middleware ───────────────────────────────────────────────────
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.url.path == "/rides" and request.method == "POST":
        client_ip = request.client.host
        key = f"rate:{client_ip}"
        count = await redis_client.incr(key)
        if count == 1:
            await redis_client.expire(key, RATE_LIMIT_WINDOW)
        if count > RATE_LIMIT_REQUESTS:
            logger.warning(
                "rate limit exceeded",
                extra={"ip": client_ip, "count": count, "dispatcher_id": DISPATCHER_ID},
            )
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded. Try again later."})
    return await call_next(request)


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.post("/rides", status_code=202)
async def submit_ride(ride: RideRequest, request: Request):
    """Client submits a ride request. Returns ride_id for polling."""
    ride_id = str(uuid.uuid4())
    payload = {
        "ride_id":      ride_id,
        "pickup":       ride.pickup,
        "dropoff":      ride.dropoff,
        "submitted_at": _now(),
        "client_ip":    request.client.host,
    }
    await redis_client.rpush("ride:queue", json.dumps(payload))
    logger.info("ride submitted", extra={"ride_id": ride_id, "dispatcher_id": DISPATCHER_ID})
    return {"ride_id": ride_id, "status": "queued"}


@app.get("/rides/{ride_id}/status")
async def ride_status(ride_id: str):
    """Client polls this endpoint until status is not 'pending'."""
    raw = await redis_client.get(f"result:{ride_id}")
    if raw is None:
        return {"ride_id": ride_id, "status": "pending"}
    data = json.loads(raw)
    return {"ride_id": ride_id, **data}


@app.post("/drivers/register", status_code=201)
async def register_driver(reg: DriverRegistration):
    """Driver nodes call this on startup to join the available pool."""
    if reg.token != DRIVER_SECRET:
        logger.warning(
            "driver registration rejected — bad token",
            extra={"driver_id": reg.driver_id, "dispatcher_id": DISPATCHER_ID},
        )
        raise HTTPException(status_code=401, detail="Invalid driver token")

    await redis_client.set(f"driver:{reg.driver_id}:url", reg.url)

    # Only add to queue if not already present (idempotent re-registration)
    existing = await redis_client.lrange("driver:queue", 0, -1)
    if reg.driver_id not in existing:
        await redis_client.rpush("driver:queue", reg.driver_id)

    logger.info(
        "driver registered",
        extra={"driver_id": reg.driver_id, "url": reg.url, "dispatcher_id": DISPATCHER_ID},
    )
    return {"status": "registered", "driver_id": reg.driver_id}

@app.post("/simulate/overload")
async def simulate_overload(count: int = 30):
    """Flood the ride queue to demonstrate load shedding behaviour."""
    import uuid
    for i in range(count):
        payload = {
            "ride_id":      str(uuid.uuid4()),
            "pickup":       f"Overload Location {i}",
            "dropoff":      "Stress Test Ave",
            "submitted_at": _now(),
            "client_ip":    "simulator",
        }
        await redis_client.rpush("ride:queue", json.dumps(payload))
    logger.warning("overload simulated", extra={"injected_rides": count, "dispatcher_id": DISPATCHER_ID})
    return {"status": "flooded", "rides_injected": count}

@app.get("/health")
async def health():
    """Observability — exposes queue depths and dispatcher state."""
    ride_depth   = await redis_client.llen("ride:queue")
    driver_count = await redis_client.llen("driver:queue")
    pending      = await redis_client.zcard("pending_assignments")
    return {
        "dispatcher_id":     DISPATCHER_ID,
        "status":            "ok",
        "ride_queue_depth":  ride_depth,
        "available_drivers": driver_count,
        "pending_assignments": pending,
        "timestamp":         _now(),
    }


# ── Dispatcher loop ────────────────────────────────────────────────────────────
async def dispatcher_loop():
    """
    Continuously BLPOP from ride:queue.
    Each ride is processed in its own async task so multiple rides
    can be in-flight simultaneously (concurrency requirement).
    """
    logger.info("dispatcher loop running", extra={"dispatcher_id": DISPATCHER_ID})
    while True:
        try:
            result = await redis_client.blpop("ride:queue", timeout=2)
            if result is None:
                continue                          # timeout — loop back and wait
            _, ride_json = result
            ride = json.loads(ride_json)
            asyncio.create_task(process_ride(ride))   # non-blocking
        except asyncio.CancelledError:
            logger.info("dispatcher loop cancelled", extra={"dispatcher_id": DISPATCHER_ID})
            break
        except Exception as exc:
            logger.error(
                "unexpected error in dispatcher loop",
                extra={"error": str(exc), "dispatcher_id": DISPATCHER_ID},
            )
            await asyncio.sleep(1)


async def process_ride(ride: dict):
    """
    Try to assign a driver to a ride.
    Retries MAX_RETRIES times across different drivers.
    Failure scenarios handled:
      - No drivers available  → re-queue ride
      - Driver HTTP timeout   → push driver back, try next
      - Driver offline        → push driver back, try next
      - Driver rejects ride   → push driver back, try next
    """
    ride_id = ride["ride_id"]
    logger.info("assigning ride", extra={"ride_id": ride_id, "dispatcher_id": DISPATCHER_ID})

    for attempt in range(1, MAX_RETRIES + 1):
        # ── Try to grab an available driver (atomic LPOP — no race condition) ──
        driver_id = await redis_client.lpop("driver:queue")

        if driver_id is None:
            logger.warning(
                "no drivers available",
                extra={"ride_id": ride_id, "attempt": attempt, "dispatcher_id": DISPATCHER_ID},
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2)
                continue
            # All retries exhausted with no driver — re-queue the ride
            await redis_client.rpush("ride:queue", json.dumps(ride))
            logger.error(
                "ride re-queued — no drivers after all retries",
                extra={"ride_id": ride_id, "dispatcher_id": DISPATCHER_ID},
            )
            return

        # ── Record this assignment attempt in sorted set (watchdog monitors this) ──
        await redis_client.zadd("pending_assignments", {driver_id: _timestamp()})

        driver_url = await redis_client.get(f"driver:{driver_id}:url")
        if not driver_url:
            logger.warning("driver url missing", extra={"driver_id": driver_id, "ride_id": ride_id})
            await redis_client.zrem("pending_assignments", driver_id)
            continue

        # ── Attempt assignment via REST ────────────────────────────────────────
        try:
            async with httpx.AsyncClient(timeout=ASSIGN_TIMEOUT) as http:
                resp = await http.post(
                    f"{driver_url}/assign",
                    json={
                        "ride_id": ride_id,
                        "pickup":  ride["pickup"],
                        "dropoff": ride["dropoff"],
                    },
                    headers={"X-Dispatcher-ID": DISPATCHER_ID},
                )

            await redis_client.zrem("pending_assignments", driver_id)

            if resp.status_code == 200 and resp.json().get("accepted"):
                # ── SUCCESS ────────────────────────────────────────────────────
                result = {
                    "status":      "assigned",
                    "driver_id":   driver_id,
                    "assigned_by": DISPATCHER_ID,
                    "assigned_at": _now(),
                }
                await redis_client.set(f"result:{ride_id}", json.dumps(result))
                logger.info(
                    "ride assigned successfully",
                    extra={"ride_id": ride_id, "driver_id": driver_id, "dispatcher_id": DISPATCHER_ID},
                )
                return

            else:
                # Driver explicitly rejected the ride — push back and try next
                logger.info(
                    "driver rejected ride",
                    extra={"driver_id": driver_id, "ride_id": ride_id, "attempt": attempt},
                )
                await redis_client.rpush("driver:queue", driver_id)

        except httpx.TimeoutException:
            await redis_client.zrem("pending_assignments", driver_id)
            logger.warning(
                "driver timed out",
                extra={"driver_id": driver_id, "ride_id": ride_id, "attempt": attempt},
            )
            await redis_client.rpush("driver:queue", driver_id)

        except httpx.ConnectError:
            await redis_client.zrem("pending_assignments", driver_id)
            logger.warning(
                "driver unreachable (offline)",
                extra={"driver_id": driver_id, "ride_id": ride_id, "attempt": attempt},
            )
            # Don't push offline driver back — they're gone

    # ── All retries failed ─────────────────────────────────────────────────────
    await redis_client.set(
        f"result:{ride_id}",
        json.dumps({"status": "failed", "reason": "no driver accepted after all retries"}),
    )
    logger.error("ride assignment failed", extra={"ride_id": ride_id, "dispatcher_id": DISPATCHER_ID})


# ── Helpers ────────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _timestamp() -> float:
    return datetime.now(timezone.utc).timestamp()
