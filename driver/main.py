import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from pythonjsonlogger import jsonlogger

# Logging
logger = logging.getLogger("driver")
_handler = logging.StreamHandler()
_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Config
DRIVER_ID       = os.getenv("DRIVER_ID", "driver-1")
DRIVER_URL      = os.getenv("DRIVER_URL", "http://driver1:8000")
DRIVER_SECRET   = os.getenv("DRIVER_SECRET", "supersecret")
DISPATCHER_URL  = os.getenv("DISPATCHER_URL", "http://dispatcher1:8000")
REJECTION_RATE  = float(os.getenv("REJECTION_RATE", "0.2"))   # 20% rejection
REGISTER_RETRY_DELAY = 3   # seconds between registration attempts

# State
driver_state = {
    "status": "available",   # available | busy
    "rides_accepted": 0,
    "rides_rejected": 0,
    "started_at":   None,
}


# Models
class AssignmentRequest(BaseModel):
    ride_id:  str
    pickup:   str
    dropoff:  str


# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    driver_state["started_at"] = _now()
    # Register with dispatcher
    asyncio.create_task(register_with_dispatcher())
    logger.info("driver node started", extra={"driver_id": DRIVER_ID})
    yield
    logger.info("driver node shutting down", extra={"driver_id": DRIVER_ID})


app = FastAPI(title=f"Driver Node [{DRIVER_ID}]", lifespan=lifespan)


# Routes
@app.post("/assign")
async def assign_ride(req: AssignmentRequest, request: Request):
    dispatcher_id = request.headers.get("X-Dispatcher-ID", "unknown")

    if getattr(app.state, "force_slow", False):
        app.state.force_slow = False
        await asyncio.sleep(10)

    # Input validation
    if not req.ride_id or not req.pickup or not req.dropoff:
        logger.warning(
            "malformed assignment request",
            extra={"driver_id": DRIVER_ID, "dispatcher_id": dispatcher_id},
        )
        raise HTTPException(status_code=422, detail="Missing required fields")

    if driver_state["status"] == "busy":
        logger.info(
            "driver busy, rejecting",
            extra={"driver_id": DRIVER_ID, "ride_id": req.ride_id},
        )
        return {"accepted": False, "reason": "driver busy"}

    # Simulate accept / reject
    if random.random() < REJECTION_RATE:
        driver_state["rides_rejected"] += 1
        logger.info(
            "driver rejected ride",
            extra={"driver_id": DRIVER_ID, "ride_id": req.ride_id, "dispatcher_id": dispatcher_id},
        )
        return {"accepted": False, "reason": "driver declined"}

    # Accept
    driver_state["status"] = "busy"
    driver_state["rides_accepted"] += 1

    logger.info(
        "driver accepted ride",
        extra={
            "driver_id":     DRIVER_ID,
            "ride_id":       req.ride_id,
            "pickup":        req.pickup,
            "dropoff":       req.dropoff,
            "dispatcher_id": dispatcher_id,
        },
    )

    # Simulate trip duration, then become available again
    asyncio.create_task(complete_ride(req.ride_id))

    return {"accepted": True, "driver_id": DRIVER_ID}


@app.get("/health")
async def health():
    return {
        "driver_id":       DRIVER_ID,
        "status":          driver_state["status"],
        "rides_accepted":  driver_state["rides_accepted"],
        "rides_rejected":  driver_state["rides_rejected"],
        "started_at":      driver_state["started_at"],
        "timestamp":       _now(),
    }

@app.post("/simulate/slow")
async def go_slow():
    global REJECTION_RATE
    app.state.force_slow = True
    return {"status": "driver will now simulate timeout on next assignment"}

# Background tasks
async def register_with_dispatcher():
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=5.0) as http:
                resp = await http.post(
                    f"{DISPATCHER_URL}/drivers/register",
                    json={
                        "driver_id": DRIVER_ID,
                        "url":       DRIVER_URL,
                        "token":     DRIVER_SECRET,
                    },
                )
            if resp.status_code == 201:
                logger.info(
                    "registered with dispatcher",
                    extra={"driver_id": DRIVER_ID, "dispatcher_url": DISPATCHER_URL},
                )
                return
            else:
                logger.warning(
                    "registration rejected",
                    extra={"driver_id": DRIVER_ID, "status_code": resp.status_code},
                )
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            logger.warning(
                "dispatcher not ready, retrying registration",
                extra={"driver_id": DRIVER_ID, "attempt": attempt + 1, "error": str(e)},
            )
        await asyncio.sleep(REGISTER_RETRY_DELAY)

    logger.error(
        "failed to register with dispatcher after all attempts",
        extra={"driver_id": DRIVER_ID},
    )


async def complete_ride(ride_id: str):
    duration = random.uniform(0.5, 2.0)
    await asyncio.sleep(duration)
    driver_state["status"] = "available"
    logger.info(
        "ride completed, driver available",
        extra={"driver_id": DRIVER_ID, "ride_id": ride_id, "trip_duration_s": round(duration, 2)},
    )
    # Re-register so dispatcher knows we're back in the pool
    await register_with_dispatcher()

# Helpers
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
