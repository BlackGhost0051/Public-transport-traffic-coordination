#!/usr/bin/env python3
"""
Agent ofert. Proponuje lokale partnerskie pasażerom czekającym na przystanku.
Uruchomienie: python offers_agent.py
"""
import argparse
import json
import time
import threading
import logging
from datetime import datetime, timedelta
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("offers")

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


def fuzzy_recommendation(wait_s: float, walk_min: float, weather_cat: str) -> float:
    wait_short  = trimf(wait_s, 0, 240, 480)
    wait_medium = trimf(wait_s, 360, 660, 900)
    wait_long   = trimf(wait_s, 720, 1200, 2400)

    walk_near   = trimf(walk_min, 0, 1, 2.5)
    walk_medium = trimf(walk_min, 2, 3.5, 5)
    walk_far    = trimf(walk_min, 4, 6, 15)

    weather_bad  = 1.0 if weather_cat in ("rain", "frost", "heat") else 0.0
    weather_good = 1.0 - weather_bad

    rules = [
        min(wait_long, walk_near),                 # 1.0 → silna
        min(wait_long, weather_bad),               # 1.0 → silna
        min(wait_medium, walk_near),               # 0.6 → średnia
        min(walk_far, 1.0 - wait_long),            # 0.1 → słaba
    ]
    weights = [1.0, 1.0, 0.6, 0.1]

    numerator = sum(r * w for r, w in zip(rules, weights))
    denominator = sum(r for r in rules if r > 0) + 1e-9
    return numerator / denominator


class OffersAgent:
    def __init__(self):
        self.id = "offers"
        self.typ = "offers"
        self.cfg = load_config()
        self.partners = self.cfg.get("partners", {})

        self.wait_times: dict = {}       # stop_id -> {line -> {wait_s, vehicle}}
        self.weather_category = "normal"
        self.partner_offers: dict = {}   # partner_id -> offer data
        self.sent_offers: dict = {}      # (stop_id, line) -> ts_sent
        self.reminder_pending: dict = {} # (stop_id, line) -> {reminder_at, text, partner}

        self.client = mqtt.Client(client_id="offers_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[offers] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("transit/stop/+/wait_time", qos=1)
            client.subscribe("transit/context/weather", qos=1)
            client.subscribe("partners/+/offer", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == "transit/context/weather":
            self.weather_category = data.get("category", "normal")

        elif topic.startswith("partners/") and topic.endswith("/offer"):
            partner_id = topic.split("/")[1]
            self.partner_offers[partner_id] = data
            log.debug("[offers] Oferta partnera %s: %s", partner_id, data.get("description", ""))

        elif topic.startswith("transit/stop/") and topic.endswith("/wait_time"):
            parts = topic.split("/")
            stop_id = parts[2]
            lines_data = data.get("lines", {})
            self.wait_times[stop_id] = lines_data
            self._evaluate_stop(stop_id, lines_data)

    def _evaluate_stop(self, stop_id: str, lines_data: dict):
        for partner_id, pinfo in self.partners.items():
            if pinfo.get("stop") != stop_id:
                continue
            walk_min = pinfo.get("walk_min", 5)
            walk_s = walk_min * 60
            offer_data = self.partner_offers.get(partner_id)

            for line, winfo in lines_data.items():
                wait_s = winfo.get("wait_s", 0)
                vehicle_id = winfo.get("vehicle", "")
                key = (stop_id, line)

                # Warunek twardy: wait ≥ 2×walk + 5min + 3min zapasu
                hard_threshold = 2 * walk_s + 300 + 180
                if wait_s < hard_threshold:
                    continue

                strength = fuzzy_recommendation(wait_s, walk_min, self.weather_category)
                if strength < 0.6:
                    continue

                now = time.time()
                last_sent = self.sent_offers.get(key, 0)
                if now - last_sent < 300:
                    continue

                ts = now_iso()
                valid_until = (datetime.utcnow() + timedelta(minutes=15)).isoformat(timespec="seconds")
                partner_name = pinfo.get("name", partner_id)
                discount = offer_data.get("discount", "10%") if offer_data else "10%"
                text = f"{partner_name} - {discount}, {walk_min} min pieszo"

                offer_msg = {
                    "ts": ts, "src": self.id,
                    "kind": "offer",
                    "partner": partner_id,
                    "text": text,
                    "valid_until": valid_until,
                    "strength": round(strength, 2)
                }
                self.client.publish(
                    f"transit/stop/{stop_id}/offer",
                    json.dumps(offer_msg),
                    qos=1
                )
                self.client.publish(
                    f"passenger/notify/{stop_id}/{line}",
                    json.dumps(offer_msg),
                    qos=1
                )
                self.sent_offers[key] = now
                log.info("[offers] Oferta dla %s/%s: %s (siła=%.2f)", stop_id, line, text, strength)

                reminder_at = now + wait_s - walk_s - 60
                self.reminder_pending[key] = {
                    "reminder_at": reminder_at,
                    "stop_id": stop_id,
                    "line": line,
                    "partner": partner_id,
                    "walk_min": walk_min
                }

    def _reminder_loop(self):
        while True:
            time.sleep(5)
            now = time.time()
            for key, r in list(self.reminder_pending.items()):
                if now >= r["reminder_at"]:
                    ts = now_iso()
                    mins_left = r["walk_min"] + 1
                    reminder_msg = {
                        "ts": ts, "src": self.id,
                        "kind": "reminder",
                        "partner": r["partner"],
                        "text": f"Wracaj, pojazd za {mins_left} min"
                    }
                    self.client.publish(
                        f"passenger/notify/{r['stop_id']}/{r['line']}",
                        json.dumps(reminder_msg),
                        qos=1
                    )
                    log.info("[offers] Przypomnienie dla %s/%s", r["stop_id"], r["line"])
                    del self.reminder_pending[key]

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
        threading.Thread(target=self._reminder_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("[offers] Agent ofert startuje")
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = OffersAgent()
    agent.run()
