#!/usr/bin/env python3
"""Symuluje lokale partnerskie publikujące oferty przez MQTT."""
import json
import time
import logging
from datetime import datetime

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("partners")

BROKER = "localhost"
PORT = 1883

PARTNERS = [
    {
        "id": "kawiarnia_1",
        "topic": "partners/kawiarnia_1/offer",
        "payload": {
            "src": "kawiarnia_1",
            "name": "Kawiarnia Centralna",
            "stop": "S4",
            "walk_min": 2,
            "discount": "20%",
            "valid_min": 15,
            "description": "Świeża kawa i ciasto"
        }
    },
    {
        "id": "restauracja_1",
        "topic": "partners/restauracja_1/offer",
        "payload": {
            "src": "restauracja_1",
            "name": "Restauracja Stare Miasto",
            "stop": "S3",
            "walk_min": 3,
            "discount": "15%",
            "valid_min": 20,
            "description": "Obiad dnia 25 PLN"
        }
    }
]


def main():
    client = mqtt.Client(client_id="partners_script")
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()
    log.info("Partnerzy startują, publikuję co 30s")
    while True:
        for p in PARTNERS:
            payload = dict(p["payload"])
            payload["ts"] = datetime.utcnow().isoformat(timespec="seconds")
            client.publish(p["topic"], json.dumps(payload), qos=1, retain=True)
            log.debug("Opublikowano ofertę %s", p["id"])
        time.sleep(30)


if __name__ == "__main__":
    main()
