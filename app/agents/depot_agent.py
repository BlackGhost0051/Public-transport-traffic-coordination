#!/usr/bin/env python3
"""
Agent zajezdni. Zarządza flotą rezerwową.
Uruchomienie: python depot_agent.py
"""
import argparse
import json
import time
import threading
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("depot")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class DepotAgent:
    def __init__(self):
        self.id = "depot"
        self.typ = "depot"
        self.cfg = load_config()

        self.reserves = {
            "R01": {"state": "available", "line": None},
            "R02": {"state": "available", "line": None},
        }
        self.pending_requests: list = []
        self.vehicle_capacities: dict = {}

        for line_id, line_cfg in self.cfg["lines"].items():
            self.vehicle_capacities[line_id] = line_cfg["capacity"]

        self.client = mqtt.Client(client_id="depot_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[depot] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("transit/line/+/reserve_request", qos=1)
            client.subscribe("transit/line/+/reserve_release", qos=1)
            client.subscribe("transit/alerts", qos=1)
            client.subscribe("transit/agent/vehicle/+/status", qos=1)
            self._publish_fleet()

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if "/reserve_request" in topic:
            parts = topic.split("/")
            line = parts[2]
            self.pending_requests.append({
                "line": line,
                "missing_seats": data.get("missing_seats", 0),
                "priority": 0,
                "ts": time.time()
            })
            log.info("[depot] Zgłoszenie rezerwy: linia %s, brak %d miejsc",
                     line, data.get("missing_seats", 0))

        elif "/reserve_release" in topic:
            parts = topic.split("/")
            line = parts[2]
            vid = data.get("vehicle")
            if vid and vid in self.reserves:
                self.reserves[vid]["state"] = "available"
                self.reserves[vid]["line"] = None
                log.info("[depot] Rezerwa %s zwolniona z linii %s", vid, line)
                self._publish_fleet()
            self.client.publish(
                f"transit/vehicle/{vid}/cmd/return_to_depot",
                json.dumps({"ts": now_iso(), "src": self.id}),
                qos=1
            )

        elif topic == "transit/alerts":
            if data.get("type") == "vehicle_down":
                line = data.get("line", "17")
                cap = self.vehicle_capacities.get(line, 90)
                self.pending_requests.append({
                    "line": line,
                    "missing_seats": cap,
                    "priority": 1,
                    "ts": time.time()
                })
                log.warning("[depot] Awaria pojazdu %s na linii %s, wysyłam zastępstwo",
                            data.get("entity"), line)

        elif "agent/vehicle" in topic and "/status" in topic:
            src = data.get("src", "")
            state = data.get("state", "")
            if src in self.reserves and state == "in_depot":
                self.reserves[src]["state"] = "available"
                self.reserves[src]["line"] = None
                self._publish_fleet()

    def _run_cycle(self):
        if not self.pending_requests:
            return
        requests = sorted(self.pending_requests, key=lambda r: (-r["priority"], -r["missing_seats"]))
        self.pending_requests.clear()

        for req in requests:
            free = [vid for vid, info in self.reserves.items() if info["state"] == "available"]
            if not free:
                log.warning("[depot] Brak dostępnych rezerw!")
                self.client.publish(
                    "transit/alerts",
                    json.dumps({"ts": now_iso(), "src": self.id,
                                "type": "no_reserves",
                                "entity": "depot",
                                "details": "Wszystkie rezerwy zajęte"}),
                    qos=1
                )
                break

            vid = free[0]
            line = req["line"]
            start_stop = self.cfg["lines"][line]["stops"][0]

            self.reserves[vid]["state"] = "dispatched"
            self.reserves[vid]["line"] = line

            ts = now_iso()
            self.client.publish(
                "transit/depot/dispatch",
                json.dumps({"ts": ts, "src": self.id,
                            "vehicle": vid, "line": line, "start_stop": start_stop}),
                qos=1
            )
            self.client.publish(
                "sim/depot/spawn",
                json.dumps({"ts": ts, "src": self.id,
                            "vehicle": vid, "line": line, "start_stop": start_stop}),
                qos=1
            )

            agent_script = Path(__file__).parent / "vehicle_agent.py"
            subprocess.Popen(
                [sys.executable, str(agent_script), "--id", vid, "--line", line],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            log.info("[depot] Wysłano rezerwę %s na linię %s, start: %s", vid, line, start_stop)
            self._publish_fleet()

    def _publish_fleet(self):
        fleet_list = [
            {"id": vid, "state": info["state"], "line": info.get("line")}
            for vid, info in self.reserves.items()
        ]
        self.client.publish(
            "transit/depot/fleet",
            json.dumps({"ts": now_iso(), "src": self.id, "reserves": fleet_list}),
            qos=1, retain=True
        )

    def _cycle_loop(self):
        while True:
            time.sleep(30)
            try:
                self._run_cycle()
            except Exception as e:
                log.error("[depot] Błąd cyklu: %s", e)

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
        log.info("[depot] Agent zajezdni startuje")
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = DepotAgent()
    agent.run()
