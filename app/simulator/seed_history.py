#!/usr/bin/env python3
"""Generuje 4 tygodnie syntetycznej historii pasazerow do SQLite."""
import sqlite3
import random
import json
import math
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = BASE_DIR / "config"
DB_FILE = DATA_DIR / "transit.db"
NETWORK_FILE = CONFIG_DIR / "network.json"

# ---------------------------------------------------------------------------
# Poisson helper
# ---------------------------------------------------------------------------

def _poisson(lam: float, rng: random.Random) -> int:
    """Draw from a Poisson distribution using the Knuth algorithm."""
    if lam <= 0:
        return 0
    L = math.exp(-min(lam, 700))
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= rng.random()
    return k - 1


# ---------------------------------------------------------------------------
# Intensity model
# ---------------------------------------------------------------------------

def _base_lambda_per_min(hour: int, minute: int, is_weekday: bool) -> float:
    """
    Returns expected passengers per minute at a stop for a given hour/minute.

    Hour bands:
        22-06: night   ~1 pax/min
        06-07: early   ~3 pax/min
        07-09: AM peak ~25 pax/min (weekday) / ~15 pax/min (weekend)
        09-16: midday  ~5 pax/min
        16-18: PM peak ~20 pax/min (weekday) / ~12 pax/min (weekend)
        18-22: evening ~6 pax/min
    """
    if 22 <= hour or hour < 6:
        base = 1.0
    elif 6 <= hour < 7:
        base = 3.0
    elif 7 <= hour < 9:
        base = 25.0 if is_weekday else 15.0
    elif 9 <= hour < 16:
        base = 5.0
    elif 16 <= hour < 18:
        base = 20.0 if is_weekday else 12.0
    elif 18 <= hour < 22:
        base = 6.0
    else:
        base = 1.0

    # Weekday morning bonus already in model; additional +30% for AM peak
    # (already included above). Just return base.
    return base


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

def create_schema(conn: sqlite3.Connection) -> None:
    """Create all 4 tables if they do not exist."""
    cur = conn.cursor()

    # 1. waiting_history – per-minute waiting counts at each stop/line
    cur.execute("""
        CREATE TABLE IF NOT EXISTS waiting_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            stop_id     TEXT    NOT NULL,
            line_id     TEXT    NOT NULL,
            waiting     INTEGER NOT NULL DEFAULT 0,
            boarded     INTEGER NOT NULL DEFAULT 0
        )
    """)

    # 2. vehicle_history – aggregated vehicle snapshot per 10-minute bucket
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            vehicle_id  TEXT    NOT NULL,
            line_id     TEXT    NOT NULL,
            passengers  INTEGER NOT NULL DEFAULT 0,
            capacity    INTEGER NOT NULL DEFAULT 1,
            speed_kmh   REAL    NOT NULL DEFAULT 0.0,
            temp_c      REAL    NOT NULL DEFAULT 20.0,
            co2_ppm     REAL    NOT NULL DEFAULT 400.0,
            lat         REAL,
            lon         REAL
        )
    """)

    # 3. events_log – event-based log (boarding/alighting at a stop)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            vehicle_id  TEXT    NOT NULL,
            stop_id     TEXT    NOT NULL,
            line_id     TEXT    NOT NULL,
            boarding    INTEGER NOT NULL DEFAULT 0,
            alighting   INTEGER NOT NULL DEFAULT 0,
            dwell_s     INTEGER NOT NULL DEFAULT 30
        )
    """)

    # 4. signal_phases – log of signal phase changes / durations
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_phases (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            signal_id   TEXT    NOT NULL,
            phase       TEXT    NOT NULL,
            duration_s  INTEGER NOT NULL DEFAULT 60,
            source      TEXT    NOT NULL DEFAULT 'schedule'
        )
    """)

    conn.commit()


# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------

def create_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wh_ts    ON waiting_history(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wh_stop  ON waiting_history(stop_id, line_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vh_ts    ON vehicle_history(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vh_vid   ON vehicle_history(vehicle_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_el_ts    ON events_log(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_ts    ON signal_phases(ts)")
    conn.commit()


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

def load_stop_lines(network_file: Path) -> dict[str, list[str]]:
    """Return dict: stop_id -> list of line_ids serving that stop."""
    with open(network_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    result = {}
    for stop_id, stop_data in data["stops"].items():
        result[stop_id] = stop_data.get("lines", [])
    return result


def load_lines_capacity(network_file: Path) -> dict[str, int]:
    """Return dict: line_id -> vehicle capacity."""
    with open(network_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {lid: ldata["capacity"] for lid, ldata in data["lines"].items()}


def load_signals(network_file: Path) -> list[str]:
    with open(network_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return list(data.get("signals", {}).keys())


# ---------------------------------------------------------------------------
# Generate waiting_history
# ---------------------------------------------------------------------------

def generate_waiting_history(
    conn: sqlite3.Connection,
    stop_lines: dict[str, list[str]],
    start_dt: datetime,
    end_dt: datetime,
    rng: random.Random,
    batch_size: int = 5000,
) -> int:
    """
    Generate per-minute waiting_history rows for all stops and lines
    between start_dt and end_dt (exclusive end).
    Returns the total number of rows inserted.
    """
    rows = []
    total = 0
    cur = conn.cursor()

    current = start_dt
    while current < end_dt:
        is_weekday = current.weekday() < 5  # 0=Mon .. 4=Fri
        hour = current.hour
        minute = current.minute
        lam = _base_lambda_per_min(hour, minute, is_weekday)

        for stop_id, lines in stop_lines.items():
            for line_id in lines:
                # Each stop/line shares the global intensity, divided among lines
                per_line_lam = lam / max(len(lines), 1)
                waiting = _poisson(per_line_lam, rng)
                # Rough boarded estimate: half of waiting (bounded by vehicle capacity)
                boarded = int(waiting * rng.uniform(0.4, 0.8))
                boarded = min(boarded, waiting)
                rows.append((
                    current.isoformat(),
                    stop_id,
                    line_id,
                    waiting,
                    boarded,
                ))

        # Flush batch
        if len(rows) >= batch_size:
            cur.executemany(
                "INSERT INTO waiting_history (ts, stop_id, line_id, waiting, boarded) "
                "VALUES (?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            total += len(rows)
            rows = []

        current += timedelta(minutes=1)

    # Insert remainder
    if rows:
        cur.executemany(
            "INSERT INTO waiting_history (ts, stop_id, line_id, waiting, boarded) "
            "VALUES (?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
        total += len(rows)

    return total


# ---------------------------------------------------------------------------
# Generate vehicle_history
# ---------------------------------------------------------------------------

def generate_vehicle_history(
    conn: sqlite3.Connection,
    network_file: Path,
    start_dt: datetime,
    end_dt: datetime,
    rng: random.Random,
    batch_size: int = 2000,
) -> int:
    """
    Generate synthetic 10-minute-bucket vehicle history for 6 vehicles.
    Returns row count inserted.
    """
    with open(network_file, "r", encoding="utf-8") as f:
        net = json.load(f)

    vehicles = [
        {"id": "A01", "line": "17"},
        {"id": "A02", "line": "17"},
        {"id": "A03", "line": "17"},
        {"id": "T01", "line": "4"},
        {"id": "T02", "line": "4"},
        {"id": "T03", "line": "4"},
    ]
    capacities = {lid: ldata["capacity"] for lid, ldata in net["lines"].items()}

    # Build a simple route geometry for interpolation
    def route_coords(line_id):
        stops = net["lines"][line_id]["stops"]
        coords = []
        for s in stops:
            coords.append((net["stops"][s]["lat"], net["stops"][s]["lon"]))
        return coords

    rows = []
    total = 0
    cur = conn.cursor()

    # Simulate initial positions spread across route
    vehicle_positions = {}
    for i, v in enumerate(vehicles):
        coords = route_coords(v["line"])
        n = len(coords)
        seg = (i * n) // len(vehicles)
        vehicle_positions[v["id"]] = {"seg": seg % max(n - 1, 1), "frac": rng.uniform(0.1, 0.9)}

    current = start_dt
    while current < end_dt:
        is_weekday = current.weekday() < 5
        hour = current.hour
        lam = _base_lambda_per_min(hour, 0, is_weekday)
        fill_target = min(lam / 25.0, 1.0)  # normalise to 0-1

        for v in vehicles:
            cap = capacities[v["line"]]
            fill = rng.uniform(max(0, fill_target - 0.15), min(1.0, fill_target + 0.15))
            pax = int(cap * fill)
            speed = rng.uniform(15.0, 35.0)
            temp = 15.0 + fill * 8.0 + rng.uniform(-0.5, 0.5)
            co2 = 400.0 + fill * 1000.0 + rng.uniform(-20, 20)

            # Simple lat/lon from route interpolation
            coords = route_coords(v["line"])
            pos = vehicle_positions[v["id"]]
            seg = pos["seg"]
            frac = pos["frac"]
            if seg < len(coords) - 1:
                lat1, lon1 = coords[seg]
                lat2, lon2 = coords[seg + 1]
                lat = lat1 + (lat2 - lat1) * frac
                lon = lon1 + (lon2 - lon1) * frac
            else:
                lat, lon = coords[-1]

            # Advance position
            frac += rng.uniform(0.1, 0.4)
            if frac >= 1.0:
                frac -= 1.0
                seg = (seg + 1) % max(len(coords) - 1, 1)
            vehicle_positions[v["id"]] = {"seg": seg, "frac": frac}

            rows.append((
                current.isoformat(),
                v["id"],
                v["line"],
                pax,
                cap,
                round(speed, 1),
                round(temp, 1),
                round(co2, 1),
                round(lat, 6),
                round(lon, 6),
            ))

        if len(rows) >= batch_size:
            cur.executemany(
                "INSERT INTO vehicle_history "
                "(ts, vehicle_id, line_id, passengers, capacity, speed_kmh, temp_c, co2_ppm, lat, lon) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            total += len(rows)
            rows = []

        current += timedelta(minutes=10)

    if rows:
        cur.executemany(
            "INSERT INTO vehicle_history "
            "(ts, vehicle_id, line_id, passengers, capacity, speed_kmh, temp_c, co2_ppm, lat, lon) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
        total += len(rows)

    return total


# ---------------------------------------------------------------------------
# Generate events_log
# ---------------------------------------------------------------------------

def generate_events_log(
    conn: sqlite3.Connection,
    stop_lines: dict[str, list[str]],
    start_dt: datetime,
    end_dt: datetime,
    rng: random.Random,
    batch_size: int = 2000,
) -> int:
    """
    Generate synthetic boarding/alighting event log.
    One event per vehicle per stop visit (every ~10 minutes per vehicle on average).
    Returns row count inserted.
    """
    vehicles = [
        ("A01", "17"), ("A02", "17"), ("A03", "17"),
        ("T01", "4"),  ("T02", "4"),  ("T03", "4"),
    ]
    capacities = {"17": 90, "4": 200}

    rows = []
    total = 0
    cur = conn.cursor()

    current = start_dt
    while current < end_dt:
        is_weekday = current.weekday() < 5
        hour = current.hour
        lam = _base_lambda_per_min(hour, 0, is_weekday)
        fill_target = min(lam / 25.0, 1.0)

        for vid, line_id in vehicles:
            cap = capacities[line_id]
            # Each vehicle visits roughly 1-2 stops per 10-minute interval
            stops_on_line = [s for s, ls in stop_lines.items() if line_id in ls]
            if not stops_on_line:
                continue
            stop_id = rng.choice(stops_on_line)
            fill = rng.uniform(max(0, fill_target - 0.2), min(1.0, fill_target + 0.2))
            boarding = int(cap * fill * rng.uniform(0.05, 0.15))
            alighting = int(cap * fill * rng.uniform(0.05, 0.15))
            dwell_s = rng.randint(20, 45)
            rows.append((
                current.isoformat(),
                vid,
                stop_id,
                line_id,
                boarding,
                alighting,
                dwell_s,
            ))

        if len(rows) >= batch_size:
            cur.executemany(
                "INSERT INTO events_log (ts, vehicle_id, stop_id, line_id, boarding, alighting, dwell_s) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            total += len(rows)
            rows = []

        current += timedelta(minutes=10)

    if rows:
        cur.executemany(
            "INSERT INTO events_log (ts, vehicle_id, stop_id, line_id, boarding, alighting, dwell_s) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
        total += len(rows)

    return total


# ---------------------------------------------------------------------------
# Generate signal_phases
# ---------------------------------------------------------------------------

def generate_signal_phases(
    conn: sqlite3.Connection,
    signal_ids: list[str],
    start_dt: datetime,
    end_dt: datetime,
    rng: random.Random,
    batch_size: int = 2000,
) -> int:
    """
    Generate scheduled signal phase log at 2-minute intervals.
    Returns row count inserted.
    """
    rows = []
    total = 0
    cur = conn.cursor()

    current = start_dt
    while current < end_dt:
        for sig_id in signal_ids:
            # Alternate green/red with some jitter
            phase = "green" if (current.minute // 2) % 2 == 0 else "red"
            duration_s = 60 + rng.randint(-15, 15)
            rows.append((
                current.isoformat(),
                sig_id,
                phase,
                duration_s,
                "schedule",
            ))

        if len(rows) >= batch_size:
            cur.executemany(
                "INSERT INTO signal_phases (ts, signal_id, phase, duration_s, source) "
                "VALUES (?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            total += len(rows)
            rows = []

        current += timedelta(minutes=2)

    if rows:
        cur.executemany(
            "INSERT INTO signal_phases (ts, signal_id, phase, duration_s, source) "
            "VALUES (?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
        total += len(rows)

    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    rng = random.Random(42)

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[seed_history] Opening database: {DB_FILE}")
    conn = sqlite3.connect(str(DB_FILE))

    # Create schema
    print("[seed_history] Creating schema...")
    create_schema(conn)
    create_indexes(conn)

    # Load network topology
    if not NETWORK_FILE.exists():
        print(f"[ERROR] Network file not found: {NETWORK_FILE}")
        conn.close()
        return

    stop_lines = load_stop_lines(NETWORK_FILE)
    signal_ids = load_signals(NETWORK_FILE)

    # 4-week window ending at the simulation start date
    end_dt = datetime(2026, 11, 3, 17, 0, 0)
    start_dt = end_dt - timedelta(weeks=4)

    print(f"[seed_history] Generating history from {start_dt.date()} to {end_dt.date()}")
    print(f"[seed_history]   stops: {list(stop_lines.keys())}")
    print(f"[seed_history]   signals: {signal_ids}")

    # 1. waiting_history
    print("[seed_history] Generating waiting_history (1-min intervals)...")
    n = generate_waiting_history(conn, stop_lines, start_dt, end_dt, rng)
    print(f"[seed_history]   -> {n:,} rows inserted into waiting_history")

    # 2. vehicle_history
    print("[seed_history] Generating vehicle_history (10-min intervals)...")
    n = generate_vehicle_history(conn, NETWORK_FILE, start_dt, end_dt, rng)
    print(f"[seed_history]   -> {n:,} rows inserted into vehicle_history")

    # 3. events_log
    print("[seed_history] Generating events_log (10-min intervals)...")
    n = generate_events_log(conn, stop_lines, start_dt, end_dt, rng)
    print(f"[seed_history]   -> {n:,} rows inserted into events_log")

    # 4. signal_phases
    print("[seed_history] Generating signal_phases (2-min intervals)...")
    n = generate_signal_phases(conn, signal_ids, start_dt, end_dt, rng)
    print(f"[seed_history]   -> {n:,} rows inserted into signal_phases")

    conn.close()

    # Quick verification
    conn2 = sqlite3.connect(str(DB_FILE))
    cur2 = conn2.cursor()
    for table in ("waiting_history", "vehicle_history", "events_log", "signal_phases"):
        cur2.execute(f"SELECT COUNT(*) FROM {table}")
        cnt = cur2.fetchone()[0]
        print(f"[seed_history] VERIFY {table}: {cnt:,} rows")
    conn2.close()

    print("[seed_history] Done.")


if __name__ == "__main__":
    main()
