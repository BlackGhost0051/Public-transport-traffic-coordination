#!/usr/bin/env python3
"""
Agent przystanku. Jeden proces na każdy przystanek.
Uruchomienie: python stop_agent.py --id S4
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
log = logging.getLogger("stop")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class StopAgent:
    def __init__(self, stop_id: str):
        self.id = stop_id
        self.cfg = load_config()
        self.stop_info = self.cfg["stops"].get(stop_id, {})
        self.lines = self.stop_info.get("lines", [])
        self.typ = "stop"

        self.waiting_count = 0
        self.eta_by_vehicle: dict = {}  # vid -> {stop, eta_s, line, ts_received}
        self.current_offer = None

        self.client = mqtt.Client(client_id=f"stop_{stop_id}")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[stop-%s] Połączono z MQTT", self.id)
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe(f"sim/stop/{self.id}/counter", qos=0)
            client.subscribe("transit/vehicle/+/eta", qos=0)
            client.subscribe(f"transit/stop/{self.id}/offer", qos=1)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == f"sim/stop/{self.id}/counter":
            self.waiting_count = data.get("count", 0)

        elif "/eta" in topic:
            src = data.get("src", "")
            line = data.get("line", "")
            etas = data.get("etas", [])
            for eta_entry in etas:
                if eta_entry.get("stop") == self.id:
                    self.eta_by_vehicle[src] = {
                        "eta_s": eta_entry["eta_s"],
                        "line": line,
                        "ts_received": time.time()
                    }

        elif topic == f"transit/stop/{self.id}/offer":
            self.current_offer = data

    def _publish_state(self):
        ts = now_iso()
        now = time.time()

        self.client.publish(
            f"transit/stop/{self.id}/waiting",
            json.dumps({"ts": ts, "src": f"stop-{self.id}", "count": self.waiting_count}),
            qos=0
        )

        # Usuń ETA starsze niż 60s
        stale = [vid for vid, info in self.eta_by_vehicle.items()
                 if now - info["ts_received"] > 60]
        for vid in stale:
            del self.eta_by_vehicle[vid]

        # Zbierz czasy oczekiwania per linia
        lines_wait = {}
        for vid, info in self.eta_by_vehicle.items():
            line = info["line"]
            if line not in self.lines:
                continue
            if line not in lines_wait or info["eta_s"] < lines_wait[line]["wait_s"]:
                lines_wait[line] = {"wait_s": info["eta_s"], "vehicle": vid}

        self.client.publish(
            f"transit/stop/{self.id}/wait_time",
            json.dumps({"ts": ts, "src": f"stop-{self.id}", "lines": lines_wait}),
            qos=1
        )

        # Buduj treść tablicy
        display_lines = []
        for line in self.lines:
            if line in lines_wait:
                w = lines_wait[line]
                mins = w["wait_s"] // 60
                secs = w["wait_s"] % 60
                display_lines.append(f"Linia {line}: {mins}:{secs:02d} min ({w['vehicle']})")
            else:
                display_lines.append(f"Linia {line}: brak danych")

        display_text = f"Przystanek {self.stop_info.get('name', self.id)}\n"
        display_text += f"Czekający: {self.waiting_count}\n"
        display_text += "\n".join(display_lines)
        if self.current_offer:
            display_text += f"\n>>> {self.current_offer.get('text', '')}"

        self.client.publish(
            f"transit/stop/{self.id}/display",
            json.dumps({"ts": ts, "src": f"stop-{self.id}", "text": display_text}),
            qos=1, retain=True
        )

    def _publish_loop(self):
        while True:
            time.sleep(5)
            self._publish_state()

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
        threading.Thread(target=self._publish_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("[stop-%s] Agent przystanku startuje", self.id)
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = StopAgent(args.id)
    agent.run()
