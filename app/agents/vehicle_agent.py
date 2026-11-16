#!/usr/bin/env python3
"""
Agent pojazdu. Jeden proces na każdy autobus lub tramwaj.
Uruchomienie: python vehicle_agent.py --id A01 --line 17
"""
import argparse
import json
import math
import time
import threading
import logging
from collections import deque
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("vehicle")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


def trimf(x, a, b, c):
    if x <= a or x >= c:
        return 0.0
    if x <= b:
        return (x - a) / (b - a) if b != a else 1.0
    return (c - x) / (c - b) if c != b else 1.0


def fuzzy_climate(temp_c, co2_ppm, occupancy_ratio):
    cold = trimf(temp_c, -5, 15, 20)
    comfort = trimf(temp_c, 18, 21, 25)
    hot = trimf(temp_c, 22, 28, 40)
    co2_low = trimf(co2_ppm, 0, 400, 900)
    co2_high = trimf(co2_ppm, 1100, 1500, 3000)
    occ_low = trimf(occupancy_ratio, -0.1, 0.15, 0.4)
    occ_high = trimf(occupancy_ratio, 0.6, 0.85, 1.1)

    rules = [
        (min(hot, occ_high), -100, 80),
        (min(hot, occ_low),  -60,  50),
        (cold,                80,  30),
        (co2_high,             0, 100),
        (min(comfort, co2_low), 0, 20),
    ]
    sum_w = sum_wh = sum_wv = 0.0
    for w, h, v in rules:
        if w > 0:
            sum_w  += w
            sum_wh += w * h
            sum_wv += w * v
    if sum_w == 0:
        return 0.0, 50.0
    return sum_wh / sum_w, sum_wv / sum_w


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class VehicleAgent:
    def __init__(self, vehicle_id: str, line: str):
        self.id = vehicle_id
        self.line = line
        self.cfg = load_config()
        self.line_cfg = self.cfg["lines"][line]
        self.stops = self.line_cfg["stops"]
        self.capacity = self.line_cfg["capacity"]
        self.typ = "vehicle"

        self.sim_time = None
        self.lat = 0.0
        self.lon = 0.0
        self.speed_kmh = 0.0
        self.passengers = 0
        self.temp_c = 20.0
        self.co2_ppm = 600.0
        self.next_stop = self.stops[0]
        self.delay_s = 0

        self.speed_history = deque(maxlen=3)
        self.hold_until_sim = None
        self.priority_allowed = False
        self.return_flag = False

        self.last_sensors_ts = 0.0
        self.last_heartbeat = 0.0

        self.client = mqtt.Client(client_id=f"vehicle_{vehicle_id}")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[%s] Połączono z MQTT", self.id)
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe(f"sim/vehicle/{self.id}/sensors", qos=0)
            client.subscribe(f"transit/vehicle/{self.id}/cmd/hold", qos=1)
            client.subscribe(f"transit/vehicle/{self.id}/cmd/priority_allowed", qos=1)
            client.subscribe(f"transit/vehicle/{self.id}/cmd/return_to_depot", qos=1)
            client.subscribe("sim/clock", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == "sim/clock":
            self.sim_time = data.get("sim_time")

        elif topic == f"sim/vehicle/{self.id}/sensors":
            self._process_sensors(data)

        elif topic == f"transit/vehicle/{self.id}/cmd/hold":
            duration = data.get("duration_s", 30)
            self.hold_until_sim = time.time() + duration / 10.0
            log.info("[%s] Polecenie postoju %ds na %s", self.id, duration, data.get("stop"))

        elif topic == f"transit/vehicle/{self.id}/cmd/priority_allowed":
            self.priority_allowed = data.get("allowed", False)
            log.info("[%s] Priorytet: %s", self.id, self.priority_allowed)

        elif topic == f"transit/vehicle/{self.id}/cmd/return_to_depot":
            self.return_flag = True
            log.info("[%s] Polecenie powrotu do zajezdni", self.id)

    def _process_sensors(self, data):
        self.lat = data.get("lat", self.lat)
        self.lon = data.get("lon", self.lon)
        spd = data.get("speed_kmh", 0.0)
        if spd > 0:
            self.speed_history.append(spd)
        self.speed_kmh = sum(self.speed_history) / len(self.speed_history) if self.speed_history else spd
        self.passengers = data.get("passengers", self.passengers)
        self.temp_c = data.get("temp_c", self.temp_c)
        self.co2_ppm = data.get("co2_ppm", self.co2_ppm)
        self.next_stop = data.get("next_stop", self.next_stop)
        ts = now_iso()

        self.client.publish(
            f"transit/vehicle/{self.id}/gps",
            json.dumps({"ts": ts, "src": self.id, "line": self.line,
                        "lat": self.lat, "lon": self.lon,
                        "speed_kmh": round(self.speed_kmh, 1),
                        "next_stop": self.next_stop}),
            qos=0
        )

        self.client.publish(
            f"transit/vehicle/{self.id}/occupancy",
            json.dumps({"ts": ts, "src": self.id, "line": self.line,
                        "passengers": self.passengers, "capacity": self.capacity}),
            qos=0
        )

        etas = self._compute_etas()
        self.client.publish(
            f"transit/vehicle/{self.id}/eta",
            json.dumps({"ts": ts, "src": self.id, "line": self.line, "etas": etas}),
            qos=0
        )

        self.delay_s = self._compute_delay(etas)
        self.client.publish(
            f"transit/vehicle/{self.id}/delay",
            json.dumps({"ts": ts, "src": self.id, "line": self.line, "delay_s": self.delay_s}),
            qos=1
        )

        self._check_priority_request(ts)
        self._publish_climate(ts)

    def _compute_etas(self):
        etas = []
        if self.next_stop not in self.stops:
            return etas
        idx = self.stops.index(self.next_stop)
        speed_ms = max(self.speed_kmh, 5.0) * 1000 / 3600
        cumulative_s = 0
        for i in range(idx, len(self.stops)):
            stop_id = self.stops[i]
            if i == idx:
                stop_data = self.cfg["stops"].get(stop_id, {})
                dist = haversine_m(self.lat, self.lon,
                                   stop_data.get("lat", self.lat),
                                   stop_data.get("lon", self.lon))
            else:
                prev_id = self.stops[i - 1]
                seg_key = f"{prev_id}-{stop_id}"
                seg = self.cfg["segments"].get(seg_key, {})
                dist = seg.get("length_m", 600)
                seg_speed = seg.get("avg_speed_kmh", 22) * 1000 / 3600
                speed_ms = seg_speed
            cumulative_s += dist / speed_ms
            etas.append({"stop": stop_id, "eta_s": int(cumulative_s)})
        return etas

    def _compute_delay(self, etas):
        if not etas:
            return 0
        headway = self._get_headway_s()
        expected = headway
        actual = etas[0].get("eta_s", 0)
        return actual - expected

    def _get_headway_s(self):
        hour_now = datetime.utcnow().hour
        hw = self.line_cfg["headway_min"]
        for slot, val in hw.items():
            start, end = slot.split("-")
            if int(start) <= hour_now < int(end):
                return val * 60
        return 600

    def _check_priority_request(self, ts):
        if not self.priority_allowed:
            return
        for seg_key, seg in self.cfg["segments"].items():
            sig_id = seg.get("signal")
            if not sig_id:
                continue
            stops_pair = seg_key.split("-")
            if self.next_stop in stops_pair:
                stop_data = self.cfg["stops"].get(self.next_stop, {})
                dist = haversine_m(self.lat, self.lon,
                                   stop_data.get("lat", self.lat),
                                   stop_data.get("lon", self.lon))
                if dist < 200:
                    self.client.publish(
                        f"transit/signal/{sig_id}/request",
                        json.dumps({"ts": ts, "src": self.id,
                                    "approach": "N",
                                    "delay_s": self.delay_s,
                                    "distance_m": int(dist)}),
                        qos=1
                    )
                    log.info("[%s] Prośba o priorytet na %s, dist=%.0fm", self.id, sig_id, dist)

    def _publish_climate(self, ts):
        occ_ratio = self.passengers / max(self.capacity, 1)
        heating, ventilation = fuzzy_climate(self.temp_c, self.co2_ppm, occ_ratio)
        self.client.publish(
            f"sim/vehicle/{self.id}/control",
            json.dumps({"ts": ts, "src": self.id,
                        "action": "climate",
                        "heating": round(heating, 1),
                        "ventilation": round(ventilation, 1)}),
            qos=0
        )
        self.client.publish(
            f"transit/vehicle/{self.id}/climate",
            json.dumps({"ts": ts, "src": self.id,
                        "heating": round(heating, 1),
                        "ventilation": round(ventilation, 1),
                        "temp_c": self.temp_c,
                        "co2_ppm": self.co2_ppm}),
            qos=0
        )

    def _heartbeat_loop(self):
        while True:
            time.sleep(10)
            self.client.publish(
                f"transit/agent/{self.typ}/{self.id}/heartbeat",
                json.dumps({"ts": now_iso(), "src": self.id}),
                qos=0
            )

    def run(self):
        self.client.connect(BROKER, PORT, keepalive=60)
        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t.start()
        log.info("[%s] Agent pojazdu startuje, linia %s", self.id, self.line)
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--line", required=True)
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = VehicleAgent(args.id, args.line)
    agent.run()
