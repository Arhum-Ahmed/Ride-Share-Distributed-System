"""
Watchdog Service
────────────────
Monitors the `pending_assignments` sorted set in Redis.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import redis.asyncio as aioredis
from pythonjsonlogger import jsonlogger

# Logging
logger = logging.getLogger("watchdog")
_handler = logging.StreamHandler()
_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Config
REDIS_URL               = os.getenv("REDIS_URL", "redis://localhost:6379")
STALE_THRESHOLD_SECONDS = int(os.getenv("STALE_THRESHOLD_SECONDS", "15"))
WATCHDOG_INTERVAL       = int(os.getenv("WATCHDOG_INTERVAL", "5"))    # how often to scan


async def rescue_stale_drivers(redis_client: aioredis.Redis):
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - STALE_THRESHOLD_SECONDS

    # ZRANGEBYSCORE returns all members with score <= cutoff
    stale_drivers = await redis_client.zrangebyscore("pending_assignments", "-inf", cutoff)

    if not stale_drivers:
        return

    for driver_id in stale_drivers:
        # Confirm driver URL still exists before restoring
        driver_url = await redis_client.get(f"driver:{driver_id}:url")
        if driver_url:
            await redis_client.rpush("driver:queue", driver_id)
            logger.warning(
                "rescued stale driver — pushed back to queue",
                extra={
                    "driver_id":         driver_id,
                    "stale_threshold_s": STALE_THRESHOLD_SECONDS,
                },
            )
        else:
            logger.warning(
                "stale driver has no registered url — discarding",
                extra={"driver_id": driver_id},
            )

        # Remove from pending regardless
        await redis_client.zrem("pending_assignments", driver_id)


async def watchdog_loop():
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info(
        "watchdog started",
        extra={
            "stale_threshold_s": STALE_THRESHOLD_SECONDS,
            "scan_interval_s":   WATCHDOG_INTERVAL,
        },
    )

    while True:
        try:
            await rescue_stale_drivers(redis_client)
        except Exception as exc:
            logger.error("watchdog error", extra={"error": str(exc)})

        await asyncio.sleep(WATCHDOG_INTERVAL)


if __name__ == "__main__":
    asyncio.run(watchdog_loop())
