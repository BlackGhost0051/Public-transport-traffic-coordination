#!/usr/bin/env python3
"""
Koordynator linii. Jeden proces na każdą linię.
Uruchomienie: python coordinator_agent.py --line 17
"""
import argparse
import json
import time
import threading
import logging
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("coordinator")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class CoordinatorAgent:
    def __init__(self, line: str):
        self.line = line
        self.cfg = load_config()
        self.line_cfg = self.cfg["lines"][line]
        self.stops = self.line_cfg["stops"]
        self.capacity = self.line_cfg["capacity"]
        self.id = f"coord-{line}"
        self.typ = "coordinator"

        self.vehicles: dict = {}   # vid -> {lat, lon, eta_dict, passengers, delay_s, status, last_update}
        self.forecasts: dict = {}  # stop_id -> {f15, f30, f60}
        self.reserves: set = set()
        self.on_hold: dict = {}    # vid -> until_real_ts
        self.priority_given: dict = {}  # vid -> bool

        self.shortage_since: float = None
        self.no_shortage_since: float = None

        self.sim_time_str = None

        self.client = mqtt.Client(client_id=self.id)
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
            client.subscribe("transit/vehicle/+/gps", qos=0)
            client.subscribe("transit/vehicle/+/occupancy", qos=0)
            client.subscribe("transit/vehicle/+/eta", qos=0)
            client.subscribe("transit/vehicle/+/delay", qos=1)
            client.subscribe("transit/agent/vehicle/+/status", qos=1)
            client.subscribe(f"transit/forecast/+/{self.line}", qos=1)
            client.subscribe("transit/depot/dispatch", qos=1)
            client.subscribe("transit/alerts", qos=1)
            client.subscribe("sim/clock", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == "sim/clock":
            self.sim_time_str = data.get("sim_time")
            return

        line = data.get("line", "")
        src = data.get("src", "")

        if topic.endswith("/gps") and line == self.line:
            if src not in self.vehicles:
                self.vehicles[src] = {}
            self.vehicles[src].update({
                "lat": data.get("lat"), "lon": data.get("lon"),
                "last_update": time.time(), "line": line,
                "status": self.vehicles[src].get("status", "online")
            })

        elif topic.endswith("/occupancy") and line == self.line:
            if src not in self.vehicles:
                self.vehicles[src] = {}
            self.vehicles[src]["passengers"] = data.get("passengers", 0)
            self.vehicles[src]["capacity"] = data.get("capacity", self.capacity)

        elif topic.endswith("/eta") and line == self.line:
            if src not in self.vehicles:
                self.vehicles[src] = {}
            eta_dict = {e["stop"]: e["eta_s"] for e in data.get("etas", [])}
            self.vehicles[src]["eta_dict"] = eta_dict
            self.vehicles[src]["last_update"] = time.time()

        elif topic.endswith("/delay") and line == self.line:
            if src not in self.vehicles:
                self.vehicles[src] = {}
            self.vehicles[src]["delay_s"] = data.get("delay_s", 0)

        elif "agent/vehicle" in topic and "/status" in topic:
            if src in self.vehicles:
                state = data.get("state", "online")
                self.vehicles[src]["status"] = state
                if state == "offline":
                    log.warning("[%s] Pojazd %s offline", self.id, src)

        elif f"transit/forecast/" in topic and f"/{self.line}" in topic:
            stop = data.get("stop")
            if stop:
                self.forecasts[stop] = {
                    "f15": data.get("f15", 0),
                    "f30": data.get("f30", 0),
                    "f60": data.get("f60", 0)
                }

        elif topic == "transit/depot/dispatch":
            if data.get("line") == self.line:
                vid = data.get("vehicle")
                self.reserves.add(vid)
                log.info("[%s] Rezerwa %s dołączyła do linii", self.id, vid)

        elif topic == "transit/alerts":
            if data.get("type") == "vehicle_down" and data.get("line") == self.line:
                vid = data.get("entity")
                if vid in self.vehicles:
                    self.vehicles[vid]["status"] = "offline"

    def _get_headway_norm_s(self):
        hour = datetime.utcnow().hour
        hw = self.line_cfg["headway_min"]
        for slot, val in hw.items():
            s, e = slot.split("-")
            if int(s) <= hour < int(e):
                return val * 60
        return 600

    def _run_cycle(self):
        norm_s = self._get_headway_norm_s()
        ts = now_iso()

        # Aktywne pojazdy linii (online)
        active = {
            vid: v for vid, v in self.vehicles.items()
            if v.get("status", "online") != "offline"
            and v.get("line") == self.line
            and "eta_dict" in v
        }

        # Sortuj pojazdy wg ETA do pierwszego wspólnego przystanku
        def first_eta(vid_v):
            eta_dict = vid_v[1].get("eta_dict", {})
            for s in self.stops:
                if s in eta_dict:
                    return eta_dict[s]
            return 9999
        sorted_vehicles = sorted(active.items(), key=first_eta)

        # Sprawdź odstępy
        for i in range(1, len(sorted_vehicles)):
            vid_a, va = sorted_vehicles[i - 1]
            vid_b, vb = sorted_vehicles[i]

            # Znajdź wspólny przystanek
            eta_a = va.get("eta_dict", {})
            eta_b = vb.get("eta_dict", {})
            common_stops = [s for s in self.stops if s in eta_a and s in eta_b]
            if not common_stops:
                continue
            stop = common_stops[0]
            gap_s = abs(eta_b[stop] - eta_a[stop])

            if gap_s < 0.5 * norm_s and vid_b not in self.on_hold:
                hold_dur = min(60, int(norm_s - gap_s))
                self.client.publish(
                    f"transit/vehicle/{vid_b}/cmd/hold",
                    json.dumps({"ts": ts, "src": self.id,
                                "stop": stop, "duration_s": hold_dur,
                                "reason": "headway<50%"}),
                    qos=1
                )
                self.on_hold[vid_b] = time.time() + hold_dur
                log.info("[%s] Postój %s na %s przez %ds", self.id, vid_b, stop, hold_dur)

                self.client.publish(
                    f"transit/vehicle/{vid_b}/cmd/decisions",
                    json.dumps({"ts": ts, "src": self.id,
                                "decision": "hold", "reason": "headway<50%"}),
                    qos=0
                )

            if gap_s > 1.5 * norm_s:
                allowed = gap_s > 1.2 * norm_s
                self.client.publish(
                    f"transit/vehicle/{vid_a}/cmd/priority_allowed",
                    json.dumps({"ts": ts, "src": self.id,
                                "allowed": allowed, "reason": "headway>150%"}),
                    qos=1
                )
                self.priority_given[vid_a] = allowed

        # Usuń wygasłe postoie
        now = time.time()
        self.on_hold = {vid: t for vid, t in self.on_hold.items() if t > now}

        # Prognoza vs wolne miejsca
        total_forecast_30 = sum(
            self.forecasts.get(s, {}).get("f30", 0)
            for s in self.stops
        )
        total_free = sum(
            (v.get("capacity", self.capacity) - v.get("passengers", 0))
            for v in active.values()
        )
        shortage = total_forecast_30 - total_free

        if shortage > 20:
            if self.shortage_since is None:
                self.shortage_since = now
            self.no_shortage_since = None
            self.client.publish(
                f"transit/line/{self.line}/reserve_request",
                json.dumps({"ts": ts, "src": self.id,
                            "missing_seats": int(shortage),
                            "horizon_min": 30,
                            "reason": "forecast>free_seats"}),
                qos=1
            )
            log.info("[%s] Zgłoszenie rezerwy, brak=%d miejsc", self.id, shortage)
        else:
            self.shortage_since = None
            if self.no_shortage_since is None:
                self.no_shortage_since = now
            if self.no_shortage_since and (now - self.no_shortage_since) > 15 * 60 / 10:
                for vid in list(self.reserves):
                    self.client.publish(
                        f"transit/line/{self.line}/reserve_release",
                        json.dumps({"ts": ts, "src": self.id,
                                    "vehicle": vid,
                                    "reason": "no_shortage_15min"}),
                        qos=1
                    )
                    self.reserves.discard(vid)
                    log.info("[%s] Zwolnienie rezerwy %s", self.id, vid)
                self.no_shortage_since = None

    def _cycle_loop(self):
        while True:
            time.sleep(10)
            try:
                self._run_cycle()
            except Exception as e:
                log.error("[%s] Błąd cyklu: %s", self.id, e)

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
        threading.Thread(target=self._cycle_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("[%s] Koordynator linii %s startuje", self.id, self.line)
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--line", required=True)
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = CoordinatorAgent(args.line)
    agent.run()
