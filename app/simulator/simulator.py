#!/usr/bin/env python3
"""
Symulator miasta dla inteligentnego systemu transportu miejskiego.
Uruchomienie: python simulator.py [--baseline] [--seed N] [--broker localhost] [--port 1883]
"""
import argparse
import json
import math
import random
import time
import threading
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    print("[WARN] paho-mqtt not installed. Running in log-only mode.")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
NETWORK_FILE = CONFIG_DIR / "network.json"
EVENTS_FILE = CONFIG_DIR / "events.json"
DB_FILE = DATA_DIR / "transit.db"

# ---------------------------------------------------------------------------
# Simulation constants
# ---------------------------------------------------------------------------
SIM_START = datetime(2026, 11, 3, 17, 0, 0)
DT_SIM_S = 10          # simulated seconds per real second
REAL_TICK_S = 1.0      # real seconds per tick
SENSOR_PUB_EVERY = 2   # real ticks between sensor publishes
STOP_PUB_EVERY = 2     # real ticks between stop-counter publishes
LOG_EVERY_S = 30       # real seconds between console log lines


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _poisson(lam: float) -> int:
    """Draw from a Poisson distribution (Knuth algorithm)."""
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def _interp_gps(lat1, lon1, lat2, lon2, frac):
    """Linear interpolation between two GPS coordinates."""
    return lat1 + (lat2 - lat1) * frac, lon1 + (lon2 - lon1) * frac


def _hour_band(dt: datetime) -> str:
    h = dt.hour
    if 6 <= h < 9:
        return "06-09"
    if 9 <= h < 15:
        return "09-15"
    if 15 <= h < 19:
        return "15-19"
    if 19 <= h < 23:
        return "19-23"
    return "night"


def _arrival_lambda(dt: datetime, raining: bool) -> float:
    """Passengers arriving at a stop per simulated minute."""
    h = dt.hour
    if 7 <= h < 9:
        base = 2.0
    elif 16 <= h < 18:
        base = 1.8
    else:
        base = 0.5
    if raining:
        base *= 1.3
    return base


# ---------------------------------------------------------------------------
# Network loader
# ---------------------------------------------------------------------------

class Network:
    def __init__(self, path: Path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.lines = data["lines"]
        self.segments = data["segments"]
        self.stops = data["stops"]
        self.signals = data["signals"]
        self.partners = data.get("partners", {})
        self.events_near = data.get("events_near", {})

    def stop_coords(self, stop_id: str):
        s = self.stops[stop_id]
        return s["lat"], s["lon"]

    def segment_key(self, from_stop: str, to_stop: str) -> str:
        return f"{from_stop}-{to_stop}"

    def segment_data(self, from_stop: str, to_stop: str) -> dict:
        key = self.segment_key(from_stop, to_stop)
        return self.segments.get(key, {"length_m": 700, "avg_speed_kmh": 22})

    def route_segments(self, line_id: str):
        """Return list of (from, to) segment pairs for a line."""
        stops = self.lines[line_id]["stops"]
        return [(stops[i], stops[i + 1]) for i in range(len(stops) - 1)]


# ---------------------------------------------------------------------------
# Events loader
# ---------------------------------------------------------------------------

class EventSchedule:
    def __init__(self, path: Path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.events = data.get("events", [])
        # Build a sorted list of weather triggers: (datetime, event_dict)
        self._wx = []
        for ev in self.events:
            for wx in ev.get("weather_scenario", []):
                ts = datetime.fromisoformat(wx["sim_time"])
                self._wx.append((ts, wx))
        self._wx.sort(key=lambda x: x[0])
        self._wx_idx = 0

    def check_weather(self, sim_time: datetime) -> dict | None:
        """Return weather event dict if one triggers at or before sim_time."""
        if self._wx_idx < len(self._wx):
            ts, wx = self._wx[self._wx_idx]
            if sim_time >= ts:
                self._wx_idx += 1
                return wx
        return None

    def active_events(self, sim_time: datetime):
        """Return list of events currently active at sim_time."""
        result = []
        for ev in self.events:
            start = datetime.fromisoformat(ev["start"])
            end = datetime.fromisoformat(ev["planned_end"])
            if start <= sim_time <= end:
                result.append(ev)
        return result


# ---------------------------------------------------------------------------
# Signal state
# ---------------------------------------------------------------------------

class Signal:
    """Simple traffic signal with configurable phase durations."""

    def __init__(self, signal_id: str, green_s: int = 60, red_s: int = 60):
        self.signal_id = signal_id
        self.green_s = green_s
        self.red_s = red_s
        self._phase = "green"
        self._remaining = green_s  # simulated seconds remaining in current phase

    def step(self, dt_s: int) -> str:
        self._remaining -= dt_s
        while self._remaining <= 0:
            if self._phase == "green":
                self._phase = "red"
                self._remaining += self.red_s
            else:
                self._phase = "green"
                self._remaining += self.green_s
        return self._phase

    def set_phase(self, phase: str, duration_s: int):
        self._phase = phase
        self._remaining = duration_s

    @property
    def phase(self):
        return self._phase


# ---------------------------------------------------------------------------
# Vehicle state
# ---------------------------------------------------------------------------

class Vehicle:
    """Represents one bus or tram in the simulation."""

    def __init__(self, vehicle_id: str, line_id: str, network: Network,
                 seg_index: int, offset_frac: float):
        self.vehicle_id = vehicle_id
        self.line_id = line_id
        self.network = network
        self.capacity = network.lines[line_id]["capacity"]

        # Route as list of stop IDs
        self.route = network.lines[line_id]["stops"]
        # Segment list: list of (from, to)
        self.segments_list = network.route_segments(line_id)
        self.num_segs = len(self.segments_list)

        # Position on route
        self.seg_index = seg_index % self.num_segs
        self.offset_frac = offset_frac   # 0.0 = at from-stop, 1.0 = at to-stop

        # Dynamic state
        self.passengers = int(self.capacity * random.uniform(0.2, 0.5))
        self.speed_kmh = 0.0
        self.temp_c = 20.0
        self.co2_ppm = 600.0
        self.lat = 0.0
        self.lon = 0.0

        # Hold state (stop at stop)
        self.hold_remaining_s = 0
        self.at_stop_id = None   # set when vehicle is at a stop

        # Boarding/alighting counters (reset each stop event)
        self.last_boarding = 0
        self.last_alighting = 0

        # External hold commands
        self._cmd_hold_s = 0
        self._cmd_hold_lock = threading.Lock()

        # Update GPS immediately
        self._update_gps()

    # ------------------------------------------------------------------
    # GPS helpers
    # ------------------------------------------------------------------

    def _update_gps(self):
        from_stop, to_stop = self.segments_list[self.seg_index]
        lat1, lon1 = self.network.stop_coords(from_stop)
        lat2, lon2 = self.network.stop_coords(to_stop)
        self.lat, self.lon = _interp_gps(lat1, lon1, lat2, lon2, self.offset_frac)

    def current_segment(self):
        return self.segments_list[self.seg_index]

    def next_stop(self) -> str:
        return self.segments_list[self.seg_index][1]

    def current_from_stop(self) -> str:
        return self.segments_list[self.seg_index][0]

    # ------------------------------------------------------------------
    # Command interface
    # ------------------------------------------------------------------

    def apply_hold(self, duration_s: int):
        with self._cmd_hold_lock:
            self._cmd_hold_s = max(self._cmd_hold_s, duration_s)

    # ------------------------------------------------------------------
    # Step
    # ------------------------------------------------------------------

    def step(self, dt_sim_s: int, sim_time: datetime,
             stop_waiting: dict, signals: dict,
             raining: bool, baseline: bool):
        """
        Advance vehicle by dt_sim_s simulated seconds.
        Returns list of stop-event dicts (boarding/alighting info).
        """
        events_out = []

        # Consume external hold command
        if not baseline:
            with self._cmd_hold_lock:
                if self._cmd_hold_s > 0:
                    self.hold_remaining_s = max(self.hold_remaining_s, self._cmd_hold_s)
                    self._cmd_hold_s = 0

        # If vehicle is currently holding at a stop
        if self.hold_remaining_s > 0:
            self.hold_remaining_s -= dt_sim_s
            self.speed_kmh = 0.0
            if self.hold_remaining_s <= 0:
                self.hold_remaining_s = 0
                self.at_stop_id = None
            self._update_gps()
            self._update_environment()
            return events_out

        # Normal movement
        seg_data = self.network.segment_data(*self.segments_list[self.seg_index])
        base_speed = seg_data["avg_speed_kmh"]

        # Check if segment has a signal and it is red
        signal_key = seg_data.get("signal")
        if signal_key and signal_key in signals:
            sig = signals[signal_key]
            # Only block when in the middle third of the segment approaching the end
            if sig.phase == "red" and 0.5 < self.offset_frac < 0.95:
                base_speed = max(base_speed * 0.3, 5.0)

        self.speed_kmh = base_speed + random.uniform(-2.0, 2.0)
        self.speed_kmh = max(5.0, self.speed_kmh)

        # Distance to cover this tick
        distance_m = (self.speed_kmh / 3.6) * dt_sim_s
        seg_length_m = seg_data["length_m"]
        progress = distance_m / seg_length_m

        self.offset_frac += progress

        # Reached end of segment -> arrive at next stop
        if self.offset_frac >= 1.0:
            self.offset_frac = 1.0
            arrived_stop = self.segments_list[self.seg_index][1]

            # Boarding & alighting
            waiting = stop_waiting.get(arrived_stop, {}).get(self.line_id, 0)
            free_seats = self.capacity - self.passengers
            boarding = min(waiting, free_seats)
            boarding = max(0, boarding + random.randint(-1, 1))
            boarding = min(boarding, free_seats)

            alighting_frac = random.uniform(0.20, 0.40)
            alighting = int(self.passengers * alighting_frac)

            self.passengers = max(0, self.passengers - alighting + boarding)
            self.passengers = min(self.passengers, self.capacity)
            self.last_boarding = boarding
            self.last_alighting = alighting

            # Deduct boarded passengers from waiting queue
            if arrived_stop in stop_waiting and self.line_id in stop_waiting[arrived_stop]:
                stop_waiting[arrived_stop][self.line_id] = max(
                    0, stop_waiting[arrived_stop][self.line_id] - boarding
                )

            # Dwell time: 20-40 simulated seconds
            dwell_s = random.randint(20, 40)
            self.hold_remaining_s = dwell_s
            self.at_stop_id = arrived_stop

            events_out.append({
                "stop": arrived_stop,
                "vehicle": self.vehicle_id,
                "boarding": boarding,
                "alighting": alighting,
            })

            # Advance to next segment
            self.seg_index = (self.seg_index + 1) % self.num_segs
            self.offset_frac = 0.0

        self._update_gps()
        self._update_environment()
        return events_out

    def _update_environment(self):
        fill = self.passengers / max(self.capacity, 1)
        noise_t = random.uniform(-0.5, 0.5)
        noise_co2 = random.uniform(-20, 20)
        self.temp_c = 15.0 + fill * 8.0 + noise_t
        self.co2_ppm = 400.0 + fill * 1000.0 + noise_co2

    def to_sensor_dict(self, sim_time: datetime) -> dict:
        return {
            "ts": sim_time.isoformat(),
            "src": "sim",
            "vehicle": self.vehicle_id,
            "line": self.line_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_kmh": round(self.speed_kmh, 1),
            "passengers": self.passengers,
            "capacity": self.capacity,
            "boarding": self.last_boarding,
            "alighting": self.last_alighting,
            "temp_c": round(self.temp_c, 1),
            "co2_ppm": round(self.co2_ppm, 1),
            "next_stop": self.next_stop(),
            "stop_id": self.at_stop_id,
        }


# ---------------------------------------------------------------------------
# Stop counter
# ---------------------------------------------------------------------------

class StopState:
    """Tracks waiting passengers at a stop per line."""

    def __init__(self, stop_id: str, lines: list[str]):
        self.stop_id = stop_id
        self.lines = lines
        self.waiting: dict[str, int] = {line: 0 for line in lines}

    def total_waiting(self) -> int:
        return sum(self.waiting.values())

    def add_arrivals(self, sim_time: datetime, dt_sim_s: int, raining: bool):
        """Add Poisson-sampled new arrivals for each line."""
        lam_per_min = _arrival_lambda(sim_time, raining)
        # dt_sim_s seconds -> fraction of a minute
        lam = lam_per_min * (dt_sim_s / 60.0) / max(len(self.lines), 1)
        for line in self.lines:
            self.waiting[line] += _poisson(lam)

    def to_counter_dict(self, sim_time: datetime) -> dict:
        return {
            "ts": sim_time.isoformat(),
            "src": "sim",
            "stop": self.stop_id,
            "count": self.total_waiting(),
        }


# ---------------------------------------------------------------------------
# Main Simulator
# ---------------------------------------------------------------------------

class Simulator:
    def __init__(self, baseline: bool, seed: int, broker: str, port: int):
        self.baseline = baseline
        self.rng_seed = seed
        random.seed(seed)

        self.network = Network(NETWORK_FILE)
        self.event_schedule = EventSchedule(EVENTS_FILE)

        self.sim_time = SIM_START
        self.raining = False
        self.rain_intensity = 0.0
        self.temp_outside_c = 10.0

        # Build signals
        self.signals: dict[str, Signal] = {}
        for sig_id in self.network.signals:
            self.signals[sig_id] = Signal(sig_id, green_s=60, red_s=45)

        # Build stop states
        self.stop_states: dict[str, StopState] = {}
        for stop_id, stop_data in self.network.stops.items():
            lines = stop_data.get("lines", [])
            self.stop_states[stop_id] = StopState(stop_id, lines)

        # Build a shared waiting dict for vehicles to consume
        # Format: {stop_id: {line_id: count}}
        self.stop_waiting: dict[str, dict[str, int]] = {
            s: {l: 0 for l in self.stop_states[s].lines}
            for s in self.stop_states
        }

        # Spawn initial vehicles
        self.vehicles: list[Vehicle] = []
        self._spawn_initial_vehicles()

        # MQTT
        self.broker = broker
        self.port = port
        self.client = None
        self._mqtt_connected = False

        # Threading
        self._running = False
        self._tick_count = 0
        self._last_log_real = time.time()

        # Statistics
        self.total_boardings = 0
        self.total_alightings = 0

    # ------------------------------------------------------------------
    # Vehicle spawning
    # ------------------------------------------------------------------

    def _spawn_initial_vehicles(self):
        """Spawn 3 buses on line 17 and 3 trams on line 4."""
        for i, vid in enumerate(["A01", "A02", "A03"]):
            line = "17"
            segs = self.network.route_segments(line)
            num_segs = len(segs)
            seg_index = (i * num_segs) // 3
            offset = random.uniform(0.1, 0.9)
            v = Vehicle(vid, line, self.network, seg_index, offset)
            self.vehicles.append(v)

        for i, vid in enumerate(["T01", "T02", "T03"]):
            line = "4"
            segs = self.network.route_segments(line)
            num_segs = len(segs)
            seg_index = (i * num_segs) // 3
            offset = random.uniform(0.1, 0.9)
            v = Vehicle(vid, line, self.network, seg_index, offset)
            self.vehicles.append(v)

    def _spawn_extra_vehicle(self, payload: dict):
        """Handle sim/depot/spawn command."""
        vid = payload.get("vehicle_id", f"X{random.randint(10,99)}")
        line = payload.get("line", "17")
        if line not in self.network.lines:
            print(f"[WARN] Cannot spawn vehicle on unknown line {line}")
            return
        v = Vehicle(vid, line, self.network, 0, 0.0)
        self.vehicles.append(v)
        print(f"[SIM] Spawned new vehicle {vid} on line {line}")

    # ------------------------------------------------------------------
    # MQTT setup
    # ------------------------------------------------------------------

    def _setup_mqtt(self):
        if not MQTT_AVAILABLE:
            return
        self.client = mqtt.Client(client_id="sim_main", protocol=mqtt.MQTTv311)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            self._mqtt_connected = True
            print(f"[SIM] MQTT connected to {self.broker}:{self.port}")
        except Exception as e:
            print(f"[WARN] MQTT connect failed: {e}. Running without MQTT.")
            self._mqtt_connected = False

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("[SIM] MQTT broker handshake OK")
            # Subscribe to control topics
            client.subscribe("sim/vehicle/+/control")
            client.subscribe("sim/depot/spawn")
            client.subscribe("sim/signal/+/phase")
        else:
            print(f"[WARN] MQTT connect rc={rc}")

    def _on_message(self, client, userdata, msg):
        if self.baseline:
            return  # Ignore all commands in baseline mode
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return

        # sim/vehicle/{id}/control
        if topic.startswith("sim/vehicle/") and topic.endswith("/control"):
            parts = topic.split("/")
            vid = parts[2]
            action = payload.get("action")
            if action == "hold":
                duration_s = int(payload.get("duration_s", 30))
                for v in self.vehicles:
                    if v.vehicle_id == vid:
                        v.apply_hold(duration_s)
                        print(f"[CMD] Hold {vid} for {duration_s}s (sim)")
                        break

        # sim/depot/spawn
        elif topic == "sim/depot/spawn":
            self._spawn_extra_vehicle(payload)

        # sim/signal/{id}/phase
        elif topic.startswith("sim/signal/") and topic.endswith("/phase"):
            parts = topic.split("/")
            sig_id = parts[2]
            if sig_id in self.signals:
                phase = payload.get("phase", "green")
                duration_s = int(payload.get("duration_s", 60))
                self.signals[sig_id].set_phase(phase, duration_s)
                print(f"[CMD] Signal {sig_id} -> {phase} for {duration_s}s")

    def _publish(self, topic: str, payload: dict, qos: int = 0, retain: bool = False):
        if self._mqtt_connected and self.client:
            try:
                self.client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
            except Exception as e:
                print(f"[WARN] MQTT publish error on {topic}: {e}")

    # ------------------------------------------------------------------
    # Main loop helpers
    # ------------------------------------------------------------------

    def _advance_weather(self):
        wx = self.event_schedule.check_weather(self.sim_time)
        if wx:
            event = wx.get("event", "")
            if "rain_start" in event:
                self.raining = True
                self.rain_intensity = wx.get("precip_mm_h", 1.0)
                self.temp_outside_c = wx.get("temp_c", self.temp_outside_c)
                print(f"[SIM] Weather update: rain started at {self.sim_time.isoformat()}, "
                      f"{self.rain_intensity} mm/h, temp {self.temp_outside_c}°C")
            elif "rain_stop" in event:
                self.raining = False
                self.rain_intensity = 0.0

    def _update_stops(self):
        """Add Poisson arrivals and sync with stop_waiting dict."""
        for stop_id, ss in self.stop_states.items():
            ss.add_arrivals(self.sim_time, DT_SIM_S, self.raining)
            for line in ss.lines:
                self.stop_waiting[stop_id][line] = ss.waiting.get(line, 0)

    def _update_signals(self):
        for sig in self.signals.values():
            sig.step(DT_SIM_S)

    def _step_vehicles(self):
        for v in self.vehicles:
            events = v.step(
                DT_SIM_S, self.sim_time,
                self.stop_waiting, self.signals,
                self.raining, self.baseline
            )
            for ev in events:
                self.total_boardings += ev["boarding"]
                self.total_alightings += ev["alighting"]
                # Sync stop_states from stop_waiting
                stop_id = ev["stop"]
                if stop_id in self.stop_states:
                    for line_id in self.stop_states[stop_id].lines:
                        self.stop_states[stop_id].waiting[line_id] = \
                            self.stop_waiting[stop_id].get(line_id, 0)

    def _publish_clock(self):
        payload = {
            "ts": datetime.utcnow().isoformat(),
            "src": "sim",
            "sim_time": self.sim_time.isoformat(),
        }
        self._publish("sim/clock", payload, qos=1, retain=True)

    def _publish_sensors(self):
        for v in self.vehicles:
            payload = v.to_sensor_dict(self.sim_time)
            self._publish(f"sim/vehicle/{v.vehicle_id}/sensors", payload, qos=0)

    def _publish_stop_counters(self):
        for stop_id, ss in self.stop_states.items():
            payload = ss.to_counter_dict(self.sim_time)
            self._publish(f"sim/stop/{stop_id}/counter", payload, qos=0)

    def _log_stats(self):
        now = time.time()
        if now - self._last_log_real < LOG_EVERY_S:
            return
        self._last_log_real = now
        total_pax = sum(v.passengers for v in self.vehicles)
        total_wait = sum(ss.total_waiting() for ss in self.stop_states.values())
        mode = "BASELINE" if self.baseline else "AGENT"
        print(
            f"[SIM][{mode}] sim_time={self.sim_time.strftime('%H:%M:%S')} "
            f"vehicles={len(self.vehicles)} pax_on_board={total_pax} "
            f"waiting={total_wait} boardings={self.total_boardings} "
            f"alightings={self.total_alightings} "
            f"rain={'yes' if self.raining else 'no'}"
        )

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self):
        print(f"[SIM] Starting. mode={'BASELINE' if self.baseline else 'AGENT'} "
              f"seed={self.rng_seed} sim_start={self.sim_time.isoformat()}")
        self._setup_mqtt()
        self._running = True

        try:
            while self._running:
                tick_start = time.time()

                # 1. Advance sim clock
                self.sim_time += timedelta(seconds=DT_SIM_S)

                # 2. Weather events
                self._advance_weather()

                # 3. Stop arrivals
                self._update_stops()

                # 4. Signal steps
                self._update_signals()

                # 5. Vehicle steps
                self._step_vehicles()

                # 6. Publish clock every tick
                self._publish_clock()

                # 7. Publish sensors & stop counters on schedule
                if self._tick_count % SENSOR_PUB_EVERY == 0:
                    self._publish_sensors()
                if self._tick_count % STOP_PUB_EVERY == 0:
                    self._publish_stop_counters()

                # 8. Console log
                self._log_stats()

                self._tick_count += 1

                # Sleep to maintain real-time tick rate
                elapsed = time.time() - tick_start
                sleep_s = REAL_TICK_S - elapsed
                if sleep_s > 0:
                    time.sleep(sleep_s)

        except KeyboardInterrupt:
            print("\n[SIM] Interrupted by user.")
        finally:
            self._running = False
            if self._mqtt_connected and self.client:
                self.client.loop_stop()
                self.client.disconnect()
            print("[SIM] Stopped.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Symulator inteligentnego transportu miejskiego"
    )
    parser.add_argument("--baseline", action="store_true",
                        help="Tryb baseline: ignoruj komendy agentow")
    parser.add_argument("--seed", type=int, default=42,
                        help="Ziarno generatora losowego")
    parser.add_argument("--broker", type=str, default="localhost",
                        help="Adres brokera MQTT")
    parser.add_argument("--port", type=int, default=1883,
                        help="Port brokera MQTT")
    args = parser.parse_args()

    sim = Simulator(
        baseline=args.baseline,
        seed=args.seed,
        broker=args.broker,
        port=args.port,
    )
    sim.run()


if __name__ == "__main__":
    main()
