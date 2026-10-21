"""
Distributed Railway Reservation System — FastAPI Server
========================================================
A real booking application where Distributed Computing concepts power the backend.

Demonstrates:
  • Database sharding (3 regional SQLite shards)
  • Async replication (primary → replica with simulated lag)
  • Lamport logical clocks for event ordering
  • Distributed Lock Manager (DLM) with TTL-based leases
  • Multiple concurrency-control algorithms: CAS, 2PC, Mutex
  • Simulated load-balanced app servers (round-robin)
  • Server-Sent Events (SSE) for real-time UI updates
  • Idempotency keys for safe retries
"""

import asyncio
import hashlib
import json
import os
import random
import sqlite3
import string
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Path helpers – everything is relative to this file's location
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DIR = os.path.join(BASE_DIR, "static")

# ---------------------------------------------------------------------------
# Train catalogue
# ---------------------------------------------------------------------------
TRAINS: dict[str, dict[str, Any]] = {
    "12123": {
        "name": "Deccan Queen Superfast",
        "route": "Pune Jn → Mumbai CSMT",
        "departure": "07:15",
        "arrival": "10:25",
        "duration": "3h 10m",
        "fare": 460,
        "coach": "C1",
        "total_seats": 20,
    },
    "12001": {
        "name": "Bhopal Shatabdi Express",
        "route": "New Delhi → Bhopal",
        "departure": "06:00",
        "arrival": "14:25",
        "duration": "8h 25m",
        "fare": 1485,
        "coach": "E1",
        "total_seats": 20,
    },
    "12627": {
        "name": "Karnataka Express",
        "route": "Bengaluru → New Delhi",
        "departure": "19:20",
        "arrival": "09:00 (+2)",
        "duration": "37h 40m",
        "fare": 2150,
        "coach": "B1",
        "total_seats": 20,
    },
}

# Map each train to its regional shard
TRAIN_SHARD_MAP: dict[str, str] = {
    "12123": "shard_west",
    "12001": "shard_north",
    "12627": "shard_south",
}

# ---------------------------------------------------------------------------
# Simulated app servers (round-robin load balancer)
# ---------------------------------------------------------------------------
APP_SERVERS = [
    {"name": "AppServer-Mumbai", "region": "west", "requests": 0},
    {"name": "AppServer-Delhi", "region": "north", "requests": 0},
    {"name": "AppServer-Bengaluru", "region": "south", "requests": 0},
]
_lb_index = 0  # round-robin pointer


def load_balancer_next() -> dict:
    """Pick the next app server using simple round-robin."""
    global _lb_index
    server = APP_SERVERS[_lb_index % len(APP_SERVERS)]
    _lb_index += 1
    server["requests"] += 1
    return server


# ---------------------------------------------------------------------------
# Lamport logical clock
# ---------------------------------------------------------------------------
_lamport_clock = 0
_lamport_lock = asyncio.Lock() if False else None  # created at startup


def _init_lamport_lock():
    """Initialise the asyncio lock (must be called inside a running loop)."""
    global _lamport_lock
    _lamport_lock = asyncio.Lock()


async def lamport_tick() -> int:
    """Increment and return the Lamport timestamp (thread-safe via asyncio lock)."""
    global _lamport_clock
    async with _lamport_lock:
        _lamport_clock += 1
        return _lamport_clock


def lamport_value() -> int:
    return _lamport_clock


# ---------------------------------------------------------------------------
# Distributed Lock Manager (DLM)
# ---------------------------------------------------------------------------
# In-memory dict: seat_key → {"holder": str, "expires_at": float}
_dlm_locks: dict[str, dict[str, Any]] = {}
DLM_TTL = 15.0  # seconds


async def dlm_acquire(seat_key: str, holder: str) -> tuple[bool, str]:
    """
    Try to acquire an exclusive lease on *seat_key*.
    Returns (granted: bool, message: str).
    """
    now = time.time()
    # Expire stale lock first
    if seat_key in _dlm_locks and _dlm_locks[seat_key]["expires_at"] < now:
        del _dlm_locks[seat_key]

    if seat_key in _dlm_locks:
        existing = _dlm_locks[seat_key]
        return False, f"Lock held by {existing['holder']} until {existing['expires_at']:.1f}"

    _dlm_locks[seat_key] = {"holder": holder, "expires_at": now + DLM_TTL}
    return True, "Lock granted"


def dlm_release(seat_key: str):
    """Release a DLM lock."""
    _dlm_locks.pop(seat_key, None)


def dlm_active_count() -> int:
    """Return the number of currently active (non-expired) locks."""
    now = time.time()
    return sum(1 for v in _dlm_locks.values() if v["expires_at"] >= now)


# ---------------------------------------------------------------------------
# Activity log
# ---------------------------------------------------------------------------
_activity_log: list[dict[str, Any]] = []


async def log_activity(
    source: str,
    dest: str,
    action: str,
    detail: str,
    status: str = "OK",
) -> dict:
    """Append an activity entry with a Lamport timestamp and broadcast via SSE."""
    ts = await lamport_tick()
    entry = {
        "lamport_ts": ts,
        "time": time.time(),
        "source": source,
        "dest": dest,
        "action": action,
        "detail": detail,
        "status": status,
    }
    _activity_log.append(entry)
    await sse_broadcast("activity", entry)
    return entry


# ---------------------------------------------------------------------------
# SSE (Server-Sent Events) subscriber management
# ---------------------------------------------------------------------------
_sse_subscribers: list[asyncio.Queue] = []


async def sse_broadcast(event: str, data: Any):
    """Push an event to every connected SSE subscriber."""
    payload = {"event": event, "data": data}
    dead: list[asyncio.Queue] = []
    for q in _sse_subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    # Remove overflowed / disconnected queues
    for q in dead:
        try:
            _sse_subscribers.remove(q)
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
COLUMNS = ["A", "B", "C", "D"]
ROWS = range(1, 6)  # 1-5


def _shard_path(shard_name: str, replica: bool = False) -> str:
    suffix = "_replica.db" if replica else ".db"
    return os.path.join(DATA_DIR, f"{shard_name}{suffix}")


def _get_conn(shard_name: str, replica: bool = False) -> sqlite3.Connection:
    """Return a sqlite3 Connection for the given shard."""
    path = _shard_path(shard_name, replica)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_shard(shard_name: str, coach: str):
    """Create tables and seed seat rows for a shard (primary + replica)."""
    for replica in (False, True):
        conn = _get_conn(shard_name, replica)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS seats (
                seat_id   TEXT PRIMARY KEY,
                row_num   INTEGER,
                col       TEXT,
                status    TEXT DEFAULT 'available',
                passenger_name TEXT,
                passenger_age  INTEGER,
                pnr       TEXT,
                held_until REAL,
                version   INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                pnr              TEXT PRIMARY KEY,
                idempotency_key  TEXT UNIQUE,
                seat_id          TEXT,
                passenger_name   TEXT,
                passenger_age    INTEGER,
                fare             INTEGER,
                booked_via_server    TEXT,
                booked_via_algorithm TEXT,
                lamport_ts       INTEGER,
                booked_at        REAL,
                status           TEXT DEFAULT 'confirmed'
            )
            """
        )
        # Seed seats if empty
        existing = cur.execute("SELECT COUNT(*) FROM seats").fetchone()[0]
        if existing == 0:
            for row in ROWS:
                for col in COLUMNS:
                    seat_id = f"{coach}-{row}{col}"
                    cur.execute(
                        "INSERT INTO seats (seat_id, row_num, col) VALUES (?, ?, ?)",
                        (seat_id, row, col),
                    )
        conn.commit()
        conn.close()


def init_all_shards():
    """Initialise every shard database on disk."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for train_id, shard_name in TRAIN_SHARD_MAP.items():
        coach = TRAINS[train_id]["coach"]
        _init_shard(shard_name, coach)


# ---------------------------------------------------------------------------
# Replication helper
# ---------------------------------------------------------------------------

async def async_replicate(shard_name: str, sql: str, params: tuple):
    """
    Simulate asynchronous replication to the replica database.
    Introduces a 50 ms lag to mimic real-world async replication delay.
    """
    await asyncio.sleep(0.05)
    try:
        conn = _get_conn(shard_name, replica=True)
        conn.execute(sql, params)
        conn.commit()
        conn.close()
    except Exception:
        pass  # replica failures are non-fatal in async replication


# ---------------------------------------------------------------------------
# Background task – seat hold expiry
# ---------------------------------------------------------------------------

async def seat_hold_expiry_loop():
    """
    Runs every 1 second. Finds seats with status='held' whose hold has
    expired and resets them to 'available'.
    """
    while True:
        await asyncio.sleep(1)
        now = time.time()
        for train_id, shard_name in TRAIN_SHARD_MAP.items():
            try:
                conn = _get_conn(shard_name)
                cur = conn.cursor()
                expired = cur.execute(
                    "SELECT seat_id FROM seats WHERE status='held' AND held_until < ?",
                    (now,),
                ).fetchall()
                for row in expired:
                    sid = row["seat_id"]
                    cur.execute(
                        "UPDATE seats SET status='available', passenger_name=NULL, "
                        "passenger_age=NULL, pnr=NULL, held_until=NULL WHERE seat_id=?",
                        (sid,),
                    )
                    await log_activity(
                        "HoldExpiry",
                        shard_name,
                        "EXPIRE_HOLD",
                        f"Seat {sid} hold expired, reset to available",
                    )
                    await sse_broadcast("seat_update", {"train_id": train_id, "seat_id": sid, "status": "available"})
                conn.commit()
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# PNR generator
# ---------------------------------------------------------------------------

def generate_pnr() -> str:
    """Generate a PNR like 'PNR' + 7 uppercase hex characters."""
    return "PNR" + uuid.uuid4().hex[:7].upper()


# ---------------------------------------------------------------------------
# Core booking logic (used by both /api/book and /api/stress-test)
# ---------------------------------------------------------------------------

async def _book_seat(
    train_id: str,
    seat_id: str,
    name: str,
    age: int,
    mode: str,
    algorithm: str,
    idempotency_key: Optional[str],
    server: dict,
) -> dict:
    """
    Internal booking function implementing the full distributed protocol.
    Returns a result dict with {success, pnr, message, server_name, shard_name, algorithm, double_booking}.
    """
    shard_name = TRAIN_SHARD_MAP.get(train_id)
    if not shard_name:
        return {"success": False, "message": f"Unknown train {train_id}"}

    fare = TRAINS[train_id]["fare"]
    server_name = server["name"]

    # Log: Client → AppServer
    await log_activity("Client", server_name, "ROUTE", f"Booking request for {seat_id} on {train_id} via {server_name}")

    # ---- Idempotency check ----
    if idempotency_key:
        conn = _get_conn(shard_name)
        existing = conn.execute(
            "SELECT pnr, seat_id, passenger_name FROM bookings WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        conn.close()
        if existing:
            await log_activity(
                server_name, "Client", "IDEMPOTENT_HIT",
                f"Duplicate request (key={idempotency_key[:12]}…), returning cached PNR {existing['pnr']}",
            )
            return {
                "success": True,
                "pnr": existing["pnr"],
                "message": "Idempotent duplicate — returning cached booking",
                "server_name": server_name,
                "shard_name": shard_name,
                "algorithm": algorithm,
                "double_booking": False,
            }

    # ---- Unsafe mode (deliberately racy) ----
    if mode == "unsafe":
        return await _book_unsafe(shard_name, train_id, seat_id, name, age, fare, server_name, idempotency_key)

    # ---- Safe mode ----
    if algorithm == "cas":
        return await _book_cas(shard_name, train_id, seat_id, name, age, fare, server_name, idempotency_key)
    elif algorithm == "2pc":
        return await _book_2pc(shard_name, train_id, seat_id, name, age, fare, server_name, idempotency_key)
    elif algorithm == "mutex":
        return await _book_mutex(shard_name, train_id, seat_id, name, age, fare, server_name, idempotency_key)
    else:
        return {"success": False, "message": f"Unknown algorithm: {algorithm}"}


# ---- Unsafe booking (race-condition-prone) ----

async def _book_unsafe(
    shard_name: str, train_id: str, seat_id: str,
    name: str, age: int, fare: int, server_name: str,
    idempotency_key: Optional[str],
) -> dict:
    """
    Intentionally non-atomic booking flow.
    1) SELECT status (read)
    2) Sleep 150 ms to simulate network latency — a gap where another request can sneak in
    3) UPDATE unconditionally
    This demonstrates WHY distributed locks / CAS are needed.
    """
    await log_activity(server_name, shard_name, "UNSAFE_READ", f"Non-atomic SELECT on {seat_id}")

    conn = _get_conn(shard_name)
    row = conn.execute("SELECT status FROM seats WHERE seat_id=?", (seat_id,)).fetchone()
    conn.close()

    if not row:
        return {"success": False, "message": f"Seat {seat_id} not found", "server_name": server_name,
                "shard_name": shard_name, "algorithm": "unsafe", "double_booking": False}

    status = row["status"]

    # Simulate network gap – THIS is where races happen
    await asyncio.sleep(0.15)

    if status != "available":
        await log_activity(server_name, "Client", "REJECT", f"Seat {seat_id} was {status} at read time")
        return {"success": False, "message": f"Seat {seat_id} is {status}",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "unsafe", "double_booking": False}

    # Unconditional UPDATE – no re-check of status!
    pnr = generate_pnr()
    ts = await lamport_tick()

    conn = _get_conn(shard_name)
    conn.execute(
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=?",
        (name, age, pnr, seat_id),
    )

    # Insert booking
    idem = idempotency_key or str(uuid.uuid4())
    conn.execute(
        "INSERT INTO bookings (pnr, idempotency_key, seat_id, passenger_name, passenger_age, fare, "
        "booked_via_server, booked_via_algorithm, lamport_ts, booked_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (pnr, idem, seat_id, name, age, fare, server_name, "unsafe", ts, time.time()),
    )
    conn.commit()

    # Detect double-booking: more than one confirmed booking for this seat
    dup_count = conn.execute(
        "SELECT COUNT(*) FROM bookings WHERE seat_id=? AND status='confirmed'", (seat_id,)
    ).fetchone()[0]
    double_booking = dup_count > 1
    conn.close()

    if double_booking:
        await log_activity(
            shard_name, server_name, "DOUBLE_BOOKING",
            f"⚠ CONFLICT: Seat {seat_id} booked by multiple passengers!", status="CONFLICT",
        )

    # Replicate to replica
    asyncio.create_task(async_replicate(
        shard_name,
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=?",
        (name, age, pnr, seat_id),
    ))

    await log_activity(server_name, "Client", "BOOKED", f"Seat {seat_id} → PNR {pnr} (UNSAFE)")
    await sse_broadcast("seat_update", {"train_id": train_id, "seat_id": seat_id, "status": "booked", "pnr": pnr})

    return {
        "success": True,
        "pnr": pnr,
        "message": "Booked (unsafe — no concurrency control)",
        "server_name": server_name,
        "shard_name": shard_name,
        "algorithm": "unsafe",
        "double_booking": double_booking,
    }


# ---- CAS (Atomic Compare-and-Set) ----

async def _book_cas(
    shard_name: str, train_id: str, seat_id: str,
    name: str, age: int, fare: int, server_name: str,
    idempotency_key: Optional[str],
) -> dict:
    """
    Atomic compare-and-set: the UPDATE's WHERE clause includes status='available',
    so the DB engine guarantees only one writer wins.
    """
    pnr = generate_pnr()
    ts = await lamport_tick()

    await log_activity(server_name, shard_name, "CAS_UPDATE", f"Atomic CAS on {seat_id}")

    conn = _get_conn(shard_name)
    cur = conn.execute(
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=? AND status='available'",
        (name, age, pnr, seat_id),
    )
    rows_affected = cur.rowcount

    if rows_affected == 0:
        conn.close()
        await log_activity(server_name, "Client", "CAS_REJECT", f"Seat {seat_id} not available (CAS failed)")
        return {"success": False, "message": f"Seat {seat_id} is not available (CAS atomically rejected)",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "cas", "double_booking": False}

    # Insert booking
    idem = idempotency_key or str(uuid.uuid4())
    conn.execute(
        "INSERT INTO bookings (pnr, idempotency_key, seat_id, passenger_name, passenger_age, fare, "
        "booked_via_server, booked_via_algorithm, lamport_ts, booked_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (pnr, idem, seat_id, name, age, fare, server_name, "cas", ts, time.time()),
    )
    conn.commit()
    conn.close()

    # Async replication
    asyncio.create_task(async_replicate(
        shard_name,
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=?",
        (name, age, pnr, seat_id),
    ))

    await log_activity(server_name, "Client", "BOOKED", f"Seat {seat_id} → PNR {pnr} (CAS)")
    await sse_broadcast("seat_update", {"train_id": train_id, "seat_id": seat_id, "status": "booked", "pnr": pnr})

    return {
        "success": True,
        "pnr": pnr,
        "message": "Booked successfully (CAS — atomic compare-and-set)",
        "server_name": server_name,
        "shard_name": shard_name,
        "algorithm": "cas",
        "double_booking": False,
    }


# ---- Two-Phase Commit (2PC) ----

async def _book_2pc(
    shard_name: str, train_id: str, seat_id: str,
    name: str, age: int, fare: int, server_name: str,
    idempotency_key: Optional[str],
) -> dict:
    """
    Simulated Two-Phase Commit:
      Phase 1 (PREPARE): coordinator asks the shard to vote — check if seat is available.
      Phase 2 (COMMIT/ABORT): based on the vote, either commit the booking or abort.
    """
    pnr = generate_pnr()
    ts = await lamport_tick()

    # ---- Phase 1: PREPARE ----
    await log_activity(server_name, shard_name, "2PC_PREPARE", f"Phase-1 PREPARE: can {seat_id} be booked?")

    conn = _get_conn(shard_name)
    row = conn.execute("SELECT status FROM seats WHERE seat_id=?", (seat_id,)).fetchone()

    if not row:
        conn.close()
        await log_activity(shard_name, server_name, "2PC_VOTE_ABORT", f"Seat {seat_id} not found → VOTE ABORT")
        return {"success": False, "message": f"Seat {seat_id} not found",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "2pc", "double_booking": False}

    if row["status"] != "available":
        conn.close()
        await log_activity(shard_name, server_name, "2PC_VOTE_ABORT", f"Seat {seat_id} is {row['status']} → VOTE ABORT")
        await log_activity(server_name, shard_name, "2PC_ABORT", "Phase-2 ABORT: participant voted NO")
        return {"success": False, "message": f"Seat {seat_id} is {row['status']} (2PC aborted)",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "2pc", "double_booking": False}

    await log_activity(shard_name, server_name, "2PC_VOTE_COMMIT", f"Seat {seat_id} available → VOTE COMMIT")

    # ---- Phase 2: COMMIT ----
    await log_activity(server_name, shard_name, "2PC_COMMIT", "Phase-2 COMMIT: all participants voted YES")

    # Perform the atomic CAS update (safe even in 2PC)
    cur = conn.execute(
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=? AND status='available'",
        (name, age, pnr, seat_id),
    )

    if cur.rowcount == 0:
        conn.close()
        await log_activity(server_name, "Client", "2PC_LATE_ABORT",
                           f"Seat {seat_id} was taken between PREPARE and COMMIT")
        return {"success": False, "message": f"Seat {seat_id} taken between prepare and commit",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "2pc", "double_booking": False}

    idem = idempotency_key or str(uuid.uuid4())
    conn.execute(
        "INSERT INTO bookings (pnr, idempotency_key, seat_id, passenger_name, passenger_age, fare, "
        "booked_via_server, booked_via_algorithm, lamport_ts, booked_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (pnr, idem, seat_id, name, age, fare, server_name, "2pc", ts, time.time()),
    )
    conn.commit()
    conn.close()

    # Async replication
    asyncio.create_task(async_replicate(
        shard_name,
        "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
        "WHERE seat_id=?",
        (name, age, pnr, seat_id),
    ))

    await log_activity(server_name, "Client", "BOOKED", f"Seat {seat_id} → PNR {pnr} (2PC)")
    await sse_broadcast("seat_update", {"train_id": train_id, "seat_id": seat_id, "status": "booked", "pnr": pnr})

    return {
        "success": True,
        "pnr": pnr,
        "message": "Booked successfully (Two-Phase Commit)",
        "server_name": server_name,
        "shard_name": shard_name,
        "algorithm": "2pc",
        "double_booking": False,
    }


# ---- Distributed Mutex ----

async def _book_mutex(
    shard_name: str, train_id: str, seat_id: str,
    name: str, age: int, fare: int, server_name: str,
    idempotency_key: Optional[str],
) -> dict:
    """
    Distributed Mutex via the DLM:
      1) Acquire an exclusive lock on the seat key
      2) Perform a CAS update while holding the lock
      3) Release the lock
    """
    lock_key = f"{shard_name}:{seat_id}"
    holder = f"{server_name}:{uuid.uuid4().hex[:6]}"

    await log_activity(server_name, "DLM", "MUTEX_REQUEST", f"Requesting lock on {seat_id}")

    granted, msg = await dlm_acquire(lock_key, holder)
    if not granted:
        await log_activity("DLM", server_name, "MUTEX_DENIED", f"Lock denied for {seat_id}: {msg}", status="DENIED")
        return {"success": False, "message": f"Could not acquire mutex for {seat_id}: {msg}",
                "server_name": server_name, "shard_name": shard_name, "algorithm": "mutex", "double_booking": False}

    await log_activity("DLM", server_name, "MUTEX_GRANTED", f"Lock granted on {seat_id} to {holder}")

    try:
        # Proceed with CAS update while holding the mutex
        pnr = generate_pnr()
        ts = await lamport_tick()

        conn = _get_conn(shard_name)
        cur = conn.execute(
            "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
            "WHERE seat_id=? AND status='available'",
            (name, age, pnr, seat_id),
        )

        if cur.rowcount == 0:
            conn.close()
            await log_activity(server_name, "Client", "MUTEX_REJECT", f"Seat {seat_id} not available under mutex")
            return {"success": False, "message": f"Seat {seat_id} is not available (mutex + CAS rejected)",
                    "server_name": server_name, "shard_name": shard_name, "algorithm": "mutex", "double_booking": False}

        idem = idempotency_key or str(uuid.uuid4())
        conn.execute(
            "INSERT INTO bookings (pnr, idempotency_key, seat_id, passenger_name, passenger_age, fare, "
            "booked_via_server, booked_via_algorithm, lamport_ts, booked_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (pnr, idem, seat_id, name, age, fare, server_name, "mutex", ts, time.time()),
        )
        conn.commit()
        conn.close()

        # Async replication
        asyncio.create_task(async_replicate(
            shard_name,
            "UPDATE seats SET status='booked', passenger_name=?, passenger_age=?, pnr=?, version=version+1 "
            "WHERE seat_id=?",
            (name, age, pnr, seat_id),
        ))

        await log_activity(server_name, "Client", "BOOKED", f"Seat {seat_id} → PNR {pnr} (MUTEX)")
        await sse_broadcast("seat_update", {"train_id": train_id, "seat_id": seat_id, "status": "booked", "pnr": pnr})

        return {
            "success": True,
            "pnr": pnr,
            "message": "Booked successfully (Distributed Mutex + CAS)",
            "server_name": server_name,
            "shard_name": shard_name,
            "algorithm": "mutex",
            "double_booking": False,
        }
    finally:
        dlm_release(lock_key)
        await log_activity("DLM", server_name, "MUTEX_RELEASED", f"Lock released on {seat_id}")


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class BookRequest(BaseModel):
    train_id: str
    seat_id: str
    name: str
    age: int
    mode: str = "safe"             # "safe" | "unsafe"
    algorithm: str = "cas"         # "cas" | "2pc" | "mutex"
    idempotency_key: Optional[str] = None


class StressTestRequest(BaseModel):
    train_id: str
    seat_id: str
    mode: str = "safe"
    algorithm: str = "cas"
    count: int = 10


# ---------------------------------------------------------------------------
# Application lifespan — startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB shards, Lamport lock, background hold-expiry loop."""
    _init_lamport_lock()
    init_all_shards()
    expiry_task = asyncio.create_task(seat_hold_expiry_loop())
    yield
    expiry_task.cancel()
    try:
        await expiry_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Distributed Railway Reservation System",
    description="DCS Mini-Project — Sharding, Replication, Lamport Clocks, DLM, 2PC, CAS, Mutex",
    lifespan=lifespan,
)

# Mount static files (CSS, JS, images)
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

# 1. GET /api/trains — list trains with live availability
@app.get("/api/trains")
async def get_trains():
    """Return all trains with a real-time available-seat count from each shard."""
    result = []
    for train_id, info in TRAINS.items():
        shard_name = TRAIN_SHARD_MAP[train_id]
        conn = _get_conn(shard_name)
        avail = conn.execute(
            "SELECT COUNT(*) FROM seats WHERE status='available'"
        ).fetchone()[0]
        conn.close()
        result.append({
            "train_id": train_id,
            **info,
            "available_seats": avail,
            "shard": shard_name,
        })
    return result


# 2. GET /api/seats/{train_id} — seat map + conflict detection
@app.get("/api/seats/{train_id}")
async def get_seats(train_id: str):
    """
    Return every seat for a train (from the correct shard) and detect
    double-booking conflicts in the bookings table.
    """
    shard_name = TRAIN_SHARD_MAP.get(train_id)
    if not shard_name:
        return {"error": f"Unknown train {train_id}"}

    conn = _get_conn(shard_name)
    seats = [dict(r) for r in conn.execute("SELECT * FROM seats ORDER BY row_num, col").fetchall()]

    # Detect double-booking conflicts: seat_ids with >1 confirmed booking
    conflicts = [
        r["seat_id"]
        for r in conn.execute(
            "SELECT seat_id FROM bookings WHERE status='confirmed' "
            "GROUP BY seat_id HAVING COUNT(*) > 1"
        ).fetchall()
    ]
    conn.close()

    return {
        "train_id": train_id,
        "shard": shard_name,
        "seats": seats,
        "conflicts": conflicts,
    }


# 3. POST /api/book — book a single seat
@app.post("/api/book")
async def book_seat(req: BookRequest):
    """
    Book a single seat using the specified mode and algorithm.
    Routes through the simulated load balancer first.
    """
    server = load_balancer_next()
    result = await _book_seat(
        train_id=req.train_id,
        seat_id=req.seat_id,
        name=req.name,
        age=req.age,
        mode=req.mode,
        algorithm=req.algorithm,
        idempotency_key=req.idempotency_key,
        server=server,
    )
    return result


# 4. POST /api/stress-test — concurrent stress test on a single seat
STRESS_PASSENGERS = [
    "Aarav", "Priya", "Vikram", "Ananya", "Siddharth",
    "Sneha", "Rohan", "Neha", "Aditya", "Kavita",
]


@app.post("/api/stress-test")
async def stress_test(req: StressTestRequest):
    """
    Fire *count* concurrent booking attempts at the SAME seat.
    Uses asyncio.gather so they truly overlap, exposing race conditions
    in unsafe mode and demonstrating safety in CAS / 2PC / mutex modes.
    """
    count = min(req.count, len(STRESS_PASSENGERS))

    await log_activity(
        "StressTest", "System",
        "STRESS_START",
        f"Launching {count} concurrent bookings for {req.seat_id} ({req.mode}/{req.algorithm})",
    )

    async def _attempt(idx: int):
        passenger = STRESS_PASSENGERS[idx % len(STRESS_PASSENGERS)]
        server = load_balancer_next()
        res = await _book_seat(
            train_id=req.train_id,
            seat_id=req.seat_id,
            name=passenger,
            age=random.randint(18, 65),
            mode=req.mode,
            algorithm=req.algorithm,
            idempotency_key=None,
            server=server,
        )
        return {"passenger": passenger, **res}

    tasks = [_attempt(i) for i in range(count)]
    results = await asyncio.gather(*tasks)

    booked = [r for r in results if r.get("success")]
    rejected = [r for r in results if not r.get("success")]
    double_bookings = [r for r in results if r.get("double_booking")]

    await log_activity(
        "StressTest", "System",
        "STRESS_END",
        f"Done: {len(booked)} booked, {len(rejected)} rejected, {len(double_bookings)} double-bookings",
    )

    return {
        "total": count,
        "booked_count": len(booked),
        "rejected_count": len(rejected),
        "double_booking_count": len(double_bookings),
        "results": results,
    }


# 5. POST /api/reset — reset everything
@app.post("/api/reset")
async def reset_system():
    """
    Fully reset the system:
      • All seats → available
      • All bookings deleted
      • Activity log cleared
      • DLM locks cleared
      • Load balancer counters reset
    """
    global _lamport_clock, _lb_index

    for train_id, shard_name in TRAIN_SHARD_MAP.items():
        for replica in (False, True):
            conn = _get_conn(shard_name, replica)
            conn.execute(
                "UPDATE seats SET status='available', passenger_name=NULL, "
                "passenger_age=NULL, pnr=NULL, held_until=NULL, version=0"
            )
            conn.execute("DELETE FROM bookings")
            conn.commit()
            conn.close()

    _activity_log.clear()
    _dlm_locks.clear()
    _lamport_clock = 0
    _lb_index = 0
    for s in APP_SERVERS:
        s["requests"] = 0

    await sse_broadcast("reset", {"message": "System fully reset"})
    return {"success": True, "message": "All shards, logs, and locks have been reset"}


# 6. GET /api/system-info — distributed system overview
@app.get("/api/system-info")
async def system_info():
    """Return current state of app servers, shards, Lamport clock, and DLM."""
    shards = []
    for train_id, shard_name in TRAIN_SHARD_MAP.items():
        primary_path = _shard_path(shard_name, replica=False)
        replica_path = _shard_path(shard_name, replica=True)
        shards.append({
            "shard": shard_name,
            "train_id": train_id,
            "primary": os.path.basename(primary_path),
            "replica": os.path.basename(replica_path),
        })

    return {
        "app_servers": [
            {"name": s["name"], "region": s["region"], "requests": s["requests"]}
            for s in APP_SERVERS
        ],
        "shards": shards,
        "lamport_clock": lamport_value(),
        "active_dlm_locks": dlm_active_count(),
    }


# 7. GET /api/activity — last 60 log entries
@app.get("/api/activity")
async def get_activity():
    """Return the most recent 60 activity log entries (newest last)."""
    return _activity_log[-60:]


# 8. GET /api/events — Server-Sent Events stream
@app.get("/api/events")
async def sse_stream(request: Request):
    """
    SSE endpoint. Each connected client gets its own asyncio.Queue.
    Events: seat_update, activity, reset.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    _sse_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'], default=str)}\n\n"
                except asyncio.TimeoutError:
                    # Send keep-alive comment to prevent proxy timeouts
                    yield ": keepalive\n\n"
        finally:
            # Clean up subscriber on disconnect
            try:
                _sse_subscribers.remove(queue)
            except ValueError:
                pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# 9. GET / — serve the frontend
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the main SPA frontend."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return HTMLResponse(
            "<h1>Distributed Railway Reservation System</h1>"
            "<p>Place <code>index.html</code> in the <code>static/</code> folder.</p>",
            status_code=200,
        )
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ---------------------------------------------------------------------------
# Entry point (for `python server.py`)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
