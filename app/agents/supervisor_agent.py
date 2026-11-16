#!/usr/bin/env python3
"""
Agent nadzoru. Monitoruje cały system, zapisuje historię do bazy.
Uruchomienie: python supervisor_agent.py
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
log = logging.getLogger("supervisor")

BROKER = "localhost"
PORT = 1883
DB_PATH = Path(__file__).parent.parent / "data" / "transit.db"


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


def init_db(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS waiting_history (
            ts TEXT, stop_id TEXT, line TEXT, count INTEGER, weekday INTEGER, hour INTEGER
        );
        CREATE TABLE IF NOT EXISTS vehicle_log (
            ts TEXT, vehicle_id TEXT, line TEXT, lat REAL, lon REAL, occupancy REAL, delay_s INTEGER
        );
        CREATE TABLE IF NOT EXISTS alerts (
            ts TEXT, type TEXT, entity_id TEXT, details_json TEXT
        );
        CREATE TABLE IF NOT EXISTS decisions (
            ts TEXT, agent_id TEXT, decision TEXT, reason TEXT
        );
    """)
    conn.commit()
    return conn


class SupervisorAgent:
    def __init__(self):
        self.id = "supervisor"
        self.typ = "supervisor"

        self.heartbeats: dict = {}
        self.agent_status: dict = {}
        self.last_sensors: dict = {}
        self.active_alerts: list = []
        self.vehicle_data: dict = {}

        self.total_passengers = 0
        self.active_vehicles = 0

        db_path = DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = init_db(db_path)

        self.client = mqtt.Client(client_id="supervisor_agent")
        self.client.will_set(
            f"transit/agent/{self.typ}/{self.id}/status",
            json.dumps({"ts": now_iso(), "src": self.id, "state": "offline"}),
            qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("[supervisor] Połączono z MQTT")
            client.publish(
                f"transit/agent/{self.typ}/{self.id}/status",
                json.dumps({"ts": now_iso(), "src": self.id, "state": "online"}),
                qos=1, retain=True
            )
            client.subscribe("transit/#", qos=0)
            client.subscribe("sim/vehicle/+/sensors", qos=0)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        topic = msg.topic
        src = data.get("src", "")
        ts = data.get("ts", now_iso())

        if "/heartbeat" in topic:
            self.heartbeats[src] = time.time()

        elif "/status" in topic and "agent" in topic:
            state = data.get("state", "")
            self.agent_status[src] = state
            if state == "offline":
                self._handle_agent_down(src, ts)

        elif topic.endswith("/occupancy"):
            vid = src
            line = data.get("line", "")
            passengers = data.get("passengers", 0)
            capacity = data.get("capacity", 90)
            occupancy_pct = passengers / max(capacity, 1) * 100
            self.vehicle_data[vid] = {
                "line": line, "passengers": passengers,
                "capacity": capacity, "ts": ts
            }
            if occupancy_pct > 150:
                self._raise_alert(ts, "sensor_anomaly", vid, f"Zapełnienie {occupancy_pct:.0f}% > 150%")
            try:
                self.db.execute(
                    "INSERT INTO vehicle_log VALUES (?,?,?,?,?,?,?)",
                    (ts, vid, line,
                     self.last_sensors.get(vid, {}).get("lat", 0),
                     self.last_sensors.get(vid, {}).get("lon", 0),
                     round(occupancy_pct, 1), 0)
                )
                self.db.commit()
            except Exception as e:
                log.debug("DB błąd vehicle_log: %s", e)

        elif topic.startswith("sim/vehicle/") and topic.endswith("/sensors"):
            vid = data.get("vehicle", src)
            self.last_sensors[vid] = data
            temp = data.get("temp_c", 20)
            co2 = data.get("co2_ppm", 600)
            if not (-40 <= temp <= 60):
                self._raise_alert(ts, "sensor_anomaly", vid, f"Temperatura {temp}°C poza zakresem")
            if not (300 <= co2 <= 5000):
                self._raise_alert(ts, "sensor_anomaly", vid, f"CO2 {co2}ppm poza zakresem")

        elif topic.startswith("transit/stop/") and topic.endswith("/waiting"):
            parts = topic.split("/")
            stop_id = parts[2]
            count = data.get("count", 0)
            if count > 500:
                self._raise_alert(ts, "sensor_anomaly", stop_id, f"Czekający {count} > 500")
            now_dt = datetime.utcnow()
            try:
                self.db.execute(
                    "INSERT INTO waiting_history VALUES (?,?,?,?,?,?)",
                    (ts, stop_id, "", count, now_dt.weekday(), now_dt.hour)
                )
                self.db.commit()
            except Exception as e:
                log.debug("DB błąd waiting_history: %s", e)

        elif topic.startswith("transit/alerts"):
            try:
                self.db.execute(
                    "INSERT INTO alerts VALUES (?,?,?,?)",
                    (ts, data.get("type", ""), data.get("entity", ""), json.dumps(data))
                )
                self.db.commit()
            except Exception as e:
                log.debug("DB błąd alerts: %s", e)
            if data not in self.active_alerts:
                self.active_alerts.append(data)
                if len(self.active_alerts) > 50:
                    self.active_alerts.pop(0)

    def _handle_agent_down(self, agent_id: str, ts: str):
        alert_type = "vehicle_down" if agent_id.startswith(("A", "T", "R")) else "agent_down"
        line = ""
        if alert_type == "vehicle_down":
            vdata = self.vehicle_data.get(agent_id, {})
            line = vdata.get("line", "")
        self._raise_alert(ts, alert_type, agent_id, "LWT offline", line=line)

    def _raise_alert(self, ts, atype, entity, details, line=""):
        msg = {"ts": ts, "src": self.id, "type": atype,
               "entity": entity, "line": line, "details": details}
        self.client.publish("transit/alerts", json.dumps(msg), qos=1)
        log.warning("[supervisor] ALARM %s: %s - %s", atype, entity, details)

    def _check_heartbeats(self):
        now = time.time()
        ts = now_iso()
        for agent_id, last_seen in list(self.heartbeats.items()):
            if now - last_seen > 30:
                if self.agent_status.get(agent_id) != "offline":
                    self._handle_agent_down(agent_id, ts)
                    self.agent_status[agent_id] = "offline"

    def _publish_summary(self):
        active_v = sum(1 for v in self.vehicle_data.values())
        total_pass = sum(v.get("passengers", 0) for v in self.vehicle_data.values())
        self.client.publish(
            "transit/dashboard/summary",
            json.dumps({
                "ts": now_iso(), "src": self.id,
                "active_vehicles": active_v,
                "total_passengers": total_pass,
                "active_alerts": len(self.active_alerts),
                "alerts": self.active_alerts[-5:]
            }),
            qos=1, retain=True
        )

    def _monitor_loop(self):
        while True:
            time.sleep(5)
            try:
                self._check_heartbeats()
                self._publish_summary()
            except Exception as e:
                log.error("[supervisor] Błąd: %s", e)

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
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("[supervisor] Agent nadzoru startuje")
        self.client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()
    BROKER = args.broker
    agent = SupervisorAgent()
    agent.run()
