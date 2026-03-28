"""
Client Simulator
────────────────
Submits N ride requests concurrently across two dispatchers,
polls for results, and prints evaluation metrics.

Usage:
    python main.py [--rides 20] [--concurrency 10] [--dispatcher1 http://localhost:8001] [--dispatcher2 http://localhost:8002]

This script covers the Evaluation requirement:
  - Ride request response time (end-to-end latency)
  - Assignment success rate under concurrent load
  - Comparison of load across both dispatchers
"""

import argparse
import asyncio
import random
import time
from datetime import datetime, timezone

import httpx

# ── Sample data ────────────────────────────────────────────────────────────────
PICKUP_LOCATIONS = [
    "123 Main St", "Airport Terminal 1", "Union Station",
    "City Hall", "Central Park North", "Westside Mall",
    "University Ave", "Harbor Front", "Tech District Hub",
]
DROPOFF_LOCATIONS = [
    "456 Oak Ave", "Downtown Hotel", "North Station",
    "Sports Arena", "East Side Clinic", "Suburb Plaza",
    "Convention Center", "Lakeview Apartments", "South Terminal",
]

POLL_INTERVAL   = 0.5    # seconds between status polls
POLL_TIMEOUT    = 30.0   # give up polling after this many seconds


async def submit_ride(
    http: httpx.AsyncClient,
    dispatcher_url: str,
    pickup: str,
    dropoff: str,
    ride_num: int,
) -> dict:
    """Submit one ride and poll until assigned or failed."""
    submit_start = time.monotonic()

    try:
        resp = await http.post(
            f"{dispatcher_url}/rides",
            json={"pickup": pickup, "dropoff": dropoff},
        )
        if resp.status_code == 429:
            print(f"  [ride-{ride_num:03d}] ⚠  Rate limited by {dispatcher_url}")
            return {"status": "rate_limited", "latency_s": None}
        if resp.status_code != 202:
            print(f"  [ride-{ride_num:03d}] ✗  Unexpected status {resp.status_code}")
            return {"status": "error", "latency_s": None}

        ride_id = resp.json()["ride_id"]

    except (httpx.ConnectError, httpx.TimeoutException) as e:
        print(f"  [ride-{ride_num:03d}] ✗  Cannot reach dispatcher: {e}")
        return {"status": "unreachable", "latency_s": None}

    # ── Poll for result ────────────────────────────────────────────────────────
    deadline = time.monotonic() + POLL_TIMEOUT
    while time.monotonic() < deadline:
        await asyncio.sleep(POLL_INTERVAL)
        try:
            poll = await http.get(f"{dispatcher_url}/rides/{ride_id}/status")
            data = poll.json()
        except Exception:
            continue

        if data.get("status") == "pending":
            continue

        latency = round(time.monotonic() - submit_start, 3)
        status  = data.get("status", "unknown")
        icon    = "✓" if status == "assigned" else "✗"
        driver  = data.get("driver_id", "—")
        print(f"  [ride-{ride_num:03d}] {icon}  {status:<10}  driver={driver:<10}  {latency}s")
        return {"status": status, "latency_s": latency, "driver_id": driver}

    # Timeout
    latency = round(time.monotonic() - submit_start, 3)
    print(f"  [ride-{ride_num:03d}]  timed out waiting for result  {latency}s")
    return {"status": "timeout", "latency_s": latency}


async def run_simulation(
    dispatchers: list[str],
    total_rides: int,
    concurrency: int,
):
    print(f"\n{'='*60}")
    print(f"  Ride-Request Simulation")
    print(f"  Rides: {total_rides}  |  Concurrency: {concurrency}")
    print(f"  Dispatchers: {dispatchers}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    semaphore = asyncio.Semaphore(concurrency)
    results   = []
    sim_start = time.monotonic()

    async def bounded_ride(ride_num: int, dispatcher_url: str, pickup: str, dropoff: str):
        async with semaphore:
            async with httpx.AsyncClient(timeout=10.0) as http:
                r = await submit_ride(http, dispatcher_url, pickup, dropoff, ride_num)
                results.append(r)

    tasks = [
        asyncio.create_task(
            bounded_ride(
                ride_num      = i + 1,
                dispatcher_url= random.choice(dispatchers),
                pickup        = random.choice(PICKUP_LOCATIONS),
                dropoff       = random.choice(DROPOFF_LOCATIONS),
            )
        )
        for i in range(total_rides)
    ]

    await asyncio.gather(*tasks)

    total_time = round(time.monotonic() - sim_start, 2)

    # ── Summary ────────────────────────────────────────────────────────────────
    assigned     = [r for r in results if r["status"] == "assigned"]
    failed       = [r for r in results if r["status"] == "failed"]
    rate_limited = [r for r in results if r["status"] == "rate_limited"]
    timed_out    = [r for r in results if r["status"] == "timeout"]
    latencies    = [r["latency_s"] for r in assigned if r["latency_s"] is not None]

    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print(f"  Total rides submitted : {total_rides}")
    print(f"  Assigned successfully : {len(assigned)}  ({pct(len(assigned), total_rides)}%)")
    print(f"  Failed (no driver)    : {len(failed)}  ({pct(len(failed), total_rides)}%)")
    print(f"  Rate limited          : {len(rate_limited)}")
    print(f"  Timed out             : {len(timed_out)}")
    if latencies:
        print(f"\n  Latency (assigned rides):")
        print(f"    Min  : {min(latencies):.3f}s")
        print(f"    Max  : {max(latencies):.3f}s")
        print(f"    Avg  : {sum(latencies)/len(latencies):.3f}s")
    print(f"\n  Total simulation time : {total_time}s")
    print(f"  Throughput            : {round(total_rides / total_time, 1)} rides/s")
    print(f"{'='*60}\n")


def pct(part, total):
    return round(100 * part / total) if total else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ride-request client simulator")
    parser.add_argument("--rides",       type=int,   default=20,                         help="Total rides to submit")
    parser.add_argument("--concurrency", type=int,   default=5,                          help="Max concurrent rides in-flight")
    parser.add_argument("--dispatcher1", type=str,   default="http://localhost:8001",    help="Dispatcher 1 URL")
    parser.add_argument("--dispatcher2", type=str,   default="http://localhost:8002",    help="Dispatcher 2 URL")
    args = parser.parse_args()

    asyncio.run(
        run_simulation(
            dispatchers  = [args.dispatcher1, args.dispatcher2],
            total_rides  = args.rides,
            concurrency  = args.concurrency,
        )
    )
