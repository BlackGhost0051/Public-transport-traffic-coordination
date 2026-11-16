#!/usr/bin/env python3
"""
Agent pogody.
Uruchomienie: python weather_agent.py --mode scenario
             python weather_agent.py --mode live
"""
import argparse
import json
import time
import threading
import logging
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("weather")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"
EVENTS_PATH = Path(__file__).parent.parent / "config" / "events.json"


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class WeatherAgent:
    def __init__(self, mode: str):
        self.id = "weather"
        self.typ = "weather"
        self.mode = mode

        self.sim_time_str = None
        self.current_weather = {
            "category": "normal",
            "temp_c": 15.0,
            "precip_mm_h": 0.0
        }
        self.scenario_events = []
        self._load_scenario()

        self.client = mqtt.Client(client_id="weather_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _load_scenario(self):
        try:
            with open(EVENTS_PATH) as f:
                data = json.load(f)
            for event in data.get("events", []):
                for w in event.get("weather_scenario", []):
                    self.scenario_events.append(w)
            log.info("[weather] Załadowano %d zdarzeń pogodowych ze scenariusza", len(self.scenario_events))
        except Exception as e:
            log.warning("[weather] Nie można załadować scenariusza: %s", e)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[weather] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("sim/clock", qos=1)
            self._publish_weather()

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        if msg.topic == "sim/clock":
            self.sim_time_str = data.get("sim_time")
            if self.mode == "scenario":
                self._check_scenario()

    def _check_scenario(self):
        if not self.sim_time_str:
            return
        try:
            sim_dt = datetime.fromisoformat(self.sim_time_str)
        except Exception:
            return

        for w in self.scenario_events:
            try:
                trigger_dt = datetime.fromisoformat(w["sim_time"])
            except Exception:
                continue
            if sim_dt >= trigger_dt and w.get("event") == "rain_start":
                new_cat = "rain"
                if self.current_weather["category"] != new_cat:
                    self.current_weather = {
                        "category": "rain",
                        "temp_c": w.get("temp_c", 7.0),
                        "precip_mm_h": w.get("precip_mm_h", 2.5)
                    }
                    log.info("[weather] Deszcz zaczął padać!")
                    self._publish_weather()

    def _fetch_live_weather(self):
        if not HAS_REQUESTS:
            log.warning("[weather] Brak requests, używam domyślnej pogody")
            return
        try:
            url = ("https://api.open-meteo.com/v1/forecast"
                   "?latitude=52.23&longitude=21.01"
                   "&current_weather=true"
                   "&hourly=precipitation,temperature_2m"
                   "&forecast_days=1")
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            cw = data.get("current_weather", {})
            temp = cw.get("temperature", 15.0)
            # Pobierz opady dla bieżącej godziny
            hourly = data.get("hourly", {})
            precip_list = hourly.get("precipitation", [0.0])
            hour_idx = datetime.utcnow().hour
            precip = precip_list[hour_idx] if hour_idx < len(precip_list) else 0.0

            if precip > 1.0:
                category = "rain"
            elif temp > 30.0:
                category = "heat"
            elif temp < -10.0:
                category = "frost"
            else:
                category = "normal"

            self.current_weather = {
                "category": category,
                "temp_c": temp,
                "precip_mm_h": precip
            }
            self._publish_weather()
            log.info("[weather] Pogoda live: %s, %.1f°C, %.1fmm/h", category, temp, precip)
        except Exception as e:
            log.error("[weather] Błąd pobierania pogody: %s", e)

    def _publish_weather(self):
        msg = {"ts": now_iso(), "src": self.id}
        msg.update(self.current_weather)
        self.client.publish(
            "transit/context/weather",
            json.dumps(msg),
            qos=1, retain=True
        )

    def _live_loop(self):
        while True:
            time.sleep(900)
            self._fetch_live_weather()

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
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        if self.mode == "live":
            threading.Thread(target=self._live_loop, daemon=True).start()
            self._fetch_live_weather()
        log.info("[weather] Agent pogody startuje (tryb: %s)", self.mode)
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="scenario", choices=["scenario", "live"])
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = WeatherAgent(args.mode)
    agent.run()
