#!/usr/bin/env python3
"""
Agent wydarzeń. Monitoruje wydarzenia i wykrywa rzeczywisty czas zakończenia.
Uruchomienie: python events_agent.py
"""
import argparse
import json
import time
import threading
import logging
from collections import deque
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("events")

BROKER = "localhost"
PORT = 1883
CONFIG_PATH = Path(__file__).parent.parent / "config" / "network.json"
EVENTS_PATH = Path(__file__).parent.parent / "config" / "events.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_events():
    with open(EVENTS_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


class EventsAgent:
    def __init__(self):
        self.id = "events"
        self.typ = "events"
        self.cfg = load_config()
        events_data = load_events()
        self.events_config = {e["id"]: e for e in events_data.get("events", [])}
        self.events_near = self.cfg.get("events_near", {})

        self.stop_history: dict = {sid: deque(maxlen=10) for sid in self.cfg["stops"]}
        self.waiting: dict = {}
        self.published_events: dict = {}
        self.actual_ends: dict = {}
        self.sim_time_str = None

        self.client = mqtt.Client(client_id="events_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[events] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("transit/stop/+/waiting", qos=0)
            client.subscribe("sim/clock", qos=1)
            self._publish_all_events()

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic

        if topic == "sim/clock":
            self.sim_time_str = data.get("sim_time")
            self._check_event_timing()

        elif topic.startswith("transit/stop/") and topic.endswith("/waiting"):
            parts = topic.split("/")
            stop_id = parts[2]
            count = data.get("count", 0)
            self.waiting[stop_id] = count
            if stop_id in self.stop_history:
                self.stop_history[stop_id].append(count)
            self._check_actual_end(stop_id)

    def _check_event_timing(self):
        if not self.sim_time_str:
            return
        try:
            sim_dt = datetime.fromisoformat(self.sim_time_str)
        except Exception:
            return

        for event_id, event in self.events_config.items():
            try:
                start_dt = datetime.fromisoformat(event["start"])
                planned_end_dt = datetime.fromisoformat(event["planned_end"])
            except Exception:
                continue

            if sim_dt >= start_dt:
                attendees = int(event.get("capacity", 0) * event.get("fill_ratio", 1.0))
                payload = {
                    "ts": now_iso(), "src": self.id,
                    "place": event.get("place"),
                    "type": event.get("type"),
                    "attendees": attendees,
                    "planned_end": event["planned_end"],
                    "actual_end": self.actual_ends.get(event_id)
                }
                self.client.publish(
                    f"transit/events/{event_id}",
                    json.dumps(payload),
                    qos=1, retain=True
                )

    def _check_actual_end(self, changed_stop: str):
        for event_id, event in self.events_config.items():
            if event_id in self.actual_ends:
                continue
            place = event.get("place", "")
            near_stops = self.events_near.get(place, {})
            if changed_stop not in near_stops:
                continue

            triggered = 0
            for stop_id, _ in near_stops.items():
                history = self.stop_history.get(stop_id, deque())
                current = self.waiting.get(stop_id, 0)
                if len(history) >= 3:
                    avg = sum(history) / len(history)
                    if avg > 0 and current > 3 * avg:
                        triggered += 1

            if triggered >= 2:
                actual_end = now_iso()
                self.actual_ends[event_id] = actual_end
                log.info("[events] Wykryto rzeczywisty koniec wydarzenia %s: %s", event_id, actual_end)
                self._publish_event(event_id, actual_end)

    def _publish_event(self, event_id: str, actual_end=None):
        event = self.events_config.get(event_id)
        if not event:
            return
        attendees = int(event.get("capacity", 0) * event.get("fill_ratio", 1.0))
        self.client.publish(
            f"transit/events/{event_id}",
            json.dumps({
                "ts": now_iso(), "src": self.id,
                "place": event.get("place"),
                "type": event.get("type"),
                "attendees": attendees,
                "planned_end": event["planned_end"],
                "actual_end": actual_end
            }),
            qos=1, retain=True
        )

    def _publish_all_events(self):
        for event_id in self.events_config:
            self._publish_event(event_id, self.actual_ends.get(event_id))

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
        log.info("[events] Agent wydarzeń startuje")
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = EventsAgent()
    agent.run()
