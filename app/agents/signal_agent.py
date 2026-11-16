#!/usr/bin/env python3
"""
Agent sygnalizacji świetlnej. Jeden proces na każde skrzyżowanie.
Uruchomienie: python signal_agent.py --id X02
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
log = logging.getLogger("signal")

BROKER = "localhost"
PORT = 1883


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class SignalAgent:
    def __init__(self, signal_id: str):
        self.id = signal_id
        self.typ = "signal"

        self.pending_requests: list = []
        self.current_phase = "N-S"
        self.phase_elapsed_s = 0
        self.pedestrian_elapsed_s = 0
        self.queues: dict = {}
        self.intervened_this_cycle = False
        self.last_cycle_phase = None

        self.client = mqtt.Client(client_id=f"signal_{signal_id}")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[signal-%s] Połączono z MQTT", self.id)
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe(f"transit/signal/{self.id}/request", qos=1)
            client.subscribe(f"sim/signal/{self.id}/traffic", qos=0)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == f"transit/signal/{self.id}/request":
            self.pending_requests.append({
                "src": data.get("src", ""),
                "approach": data.get("approach", "N"),
                "delay_s": data.get("delay_s", 0),
                "distance_m": data.get("distance_m", 200),
                "ts": time.time()
            })
            log.debug("[signal-%s] Prośba o priorytet od %s, opóźnienie=%ds",
                      self.id, data.get("src"), data.get("delay_s"))

        elif topic == f"sim/signal/{self.id}/traffic":
            new_phase = data.get("current_phase", self.current_phase)
            if new_phase != self.last_cycle_phase:
                self.intervened_this_cycle = False
                self.last_cycle_phase = new_phase
            self.current_phase = new_phase
            self.phase_elapsed_s = data.get("phase_elapsed_s", 0)
            self.pedestrian_elapsed_s = data.get("pedestrian_elapsed_s", 0)
            self.queues = data.get("queues", {})
            self._process_requests()

    def _process_requests(self):
        if self.intervened_this_cycle:
            return
        if self.pedestrian_elapsed_s < 15:
            self.pending_requests.clear()
            return

        now = time.time()
        fresh = [r for r in self.pending_requests if now - r["ts"] < 30]
        self.pending_requests = fresh
        if not fresh:
            return

        best = max(fresh, key=lambda r: r["delay_s"])
        approach = best["approach"]

        # Sprawdź kolejkę poprzeczną
        perpendicular = self._get_perpendicular(approach)
        perp_queue = max((self.queues.get(d, 0) for d in perpendicular), default=0)
        if perp_queue > 15:
            log.info("[signal-%s] Odrzucono priorytet dla %s: kolejka poprzeczna %d",
                     self.id, best["src"], perp_queue)
            self.pending_requests.clear()
            return

        delta = min(15, max(5, best["delay_s"] // 20))
        action = "extend_green"
        ts = now_iso()

        self.client.publish(
            f"transit/signal/{self.id}/phase",
            json.dumps({"ts": ts, "src": f"sig-{self.id}",
                        "action": action,
                        "approach": approach,
                        "delta_s": delta,
                        "reason": f"priority {best['src']}"}),
            qos=1
        )
        log.info("[signal-%s] Priorytet dla %s: %s +%ds",
                 self.id, best["src"], action, delta)
        self.intervened_this_cycle = True
        self.pending_requests.clear()

    def _get_perpendicular(self, approach: str) -> list:
        mapping = {
            "N": ["E", "W"], "S": ["E", "W"],
            "E": ["N", "S"], "W": ["N", "S"]
        }
        return mapping.get(approach, [])

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
        log.info("[signal-%s] Agent sygnalizacji startuje", self.id)
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = SignalAgent(args.id)
    agent.run()
