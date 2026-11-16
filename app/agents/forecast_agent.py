#!/usr/bin/env python3
"""
Agent prognozy popytu.
Uruchomienie: python forecast_agent.py
"""
import argparse
import json
import time
import threading
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("forecast")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"
DB_PATH = Path(__file__).parent.parent / "data" / "transit.db"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class ForecastAgent:
    def __init__(self):
        self.id = "forecast"
        self.typ = "forecast"
        self.cfg = load_config()

        self.waiting: dict = {}         # stop_id -> count
        self.weather_category = "normal"
        self.events: dict = {}          # event_id -> data
        self.sim_time_str = None

        self.client = mqtt.Client(client_id="forecast_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[forecast] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("transit/stop/+/waiting", qos=0)
            client.subscribe("transit/context/weather", qos=1)
            client.subscribe("transit/events/+", qos=1)
            client.subscribe("sim/clock", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == "sim/clock":
            self.sim_time_str = data.get("sim_time")

        elif topic.startswith("transit/stop/") and topic.endswith("/waiting"):
            parts = topic.split("/")
            stop_id = parts[2]
            self.waiting[stop_id] = data.get("count", 0)

        elif topic == "transit/context/weather":
            self.weather_category = data.get("category", "normal")

        elif topic.startswith("transit/events/"):
            event_id = topic.split("/")[2]
            self.events[event_id] = data

    def _get_base_from_db(self, stop_id: str, weekday: int, hour: int) -> float:
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            cur.execute(
                "SELECT AVG(count) FROM waiting_history WHERE stop_id=? AND weekday=? AND hour=?",
                (stop_id, weekday, hour)
            )
            row = cur.fetchone()
            conn.close()
            if row and row[0] is not None:
                return float(row[0])
        except Exception as e:
            log.debug("DB błąd: %s", e)
        return 10.0

    def _get_line_shares(self, stop_id: str) -> dict:
        lines = self.cfg["stops"].get(stop_id, {}).get("lines", [])
        if not lines:
            return {}
        share = 1.0 / len(lines)
        return {line: share for line in lines}

    def _event_component(self, stop_id: str, horizon_min: int) -> float:
        events_near = self.cfg.get("events_near", {})
        total = 0.0
        now_sim = datetime.utcnow()
        if self.sim_time_str:
            try:
                now_sim = datetime.fromisoformat(self.sim_time_str)
            except Exception:
                pass

        for event in self.events.values():
            place = event.get("place", "")
            stop_share = events_near.get(place, {}).get(stop_id, 0.0)
            if stop_share <= 0:
                continue
            attendees = event.get("attendees", 0)
            event_type = event.get("type", "match")
            planned_end_str = event.get("planned_end") or event.get("actual_end")
            if not planned_end_str:
                continue
            try:
                planned_end = datetime.fromisoformat(planned_end_str)
            except Exception:
                continue
            time_to_end_min = (planned_end - now_sim).total_seconds() / 60.0
            if time_to_end_min < -120 or time_to_end_min > 120:
                continue

            if event_type in ("match", "concert"):
                # 70% w 20 min od końca
                if 0 <= time_to_end_min <= 20 or (-horizon_min <= time_to_end_min <= 0):
                    fraction = 0.70
                else:
                    fraction = 0.05
            else:
                # targ: równomiernie przez 2h = 120 min
                fraction = horizon_min / 120.0

            total += attendees * stop_share * fraction
        return total

    def _run_forecast(self):
        ts = now_iso()
        now_dt = datetime.utcnow()
        weekday = now_dt.weekday()
        hour = now_dt.hour

        weather_mult = {"rain": 1.3, "frost": 1.2, "heat": 1.1}.get(self.weather_category, 1.0)

        for stop_id, stop_info in self.cfg["stops"].items():
            lines = stop_info.get("lines", [])
            if not lines:
                continue

            current = self.waiting.get(stop_id, 0)
            base = self._get_base_from_db(stop_id, weekday, hour)

            corrected = 0.7 * current + 0.3 * base if abs(current - base) > 5 else base
            corrected *= weather_mult

            line_shares = self._get_line_shares(stop_id)

            for line in lines:
                share = line_shares.get(line, 1.0 / len(lines))
                f15 = int(corrected * share + self._event_component(stop_id, 15) * share)
                f30 = int(corrected * share * 1.2 + self._event_component(stop_id, 30) * share)
                f60 = int(corrected * share * 1.5 + self._event_component(stop_id, 60) * share)

                self.client.publish(
                    f"transit/forecast/{stop_id}/{line}",
                    json.dumps({"ts": ts, "src": self.id,
                                "stop": stop_id, "line": line,
                                "f15": f15, "f30": f30, "f60": f60}),
                    qos=1, retain=True
                )

        log.debug("[forecast] Prognoza opublikowana dla %d przystanków", len(self.cfg["stops"]))

    def _forecast_loop(self):
        while True:
            time.sleep(60)
            try:
                self._run_forecast()
            except Exception as e:
                log.error("[forecast] Błąd: %s", e)

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
        threading.Thread(target=self._forecast_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("[forecast] Agent prognozy startuje")
        self._run_forecast()
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = ForecastAgent()
    agent.run()
