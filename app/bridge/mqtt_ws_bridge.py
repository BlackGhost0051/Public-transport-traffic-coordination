#!/usr/bin/env python3
"""
Most MQTT → WebSocket.
Subskrybuje wszystkie tematy MQTT i przesyła wiadomości do Godot przez WebSocket.
"""
import asyncio
import json
import threading
import logging
import time

import websockets
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
WS_HOST = "0.0.0.0"
WS_PORT = 8765

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("bridge")

connected_clients: set = set()
_loop: asyncio.AbstractEventLoop = None
message_queue: asyncio.Queue = None


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT połączono z %s:%d", MQTT_BROKER, MQTT_PORT)
        client.subscribe("transit/#", qos=0)
        client.subscribe("sim/#", qos=0)
        client.subscribe("partners/#", qos=0)
        client.subscribe("passenger/#", qos=0)
    else:
        log.error("MQTT błąd połączenia rc=%d", rc)


def on_mqtt_disconnect(client, userdata, rc):
    log.warning("MQTT rozłączono rc=%d, próba ponownego połączenia...", rc)
    while True:
        try:
            client.reconnect()
            break
        except Exception:
            time.sleep(3)


def on_mqtt_message(client, userdata, msg):
    global _loop, message_queue
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        payload = {"raw": msg.payload.decode("utf-8", errors="replace")}
    envelope = json.dumps({"topic": msg.topic, "payload": payload})
    if _loop and message_queue:
        asyncio.run_coroutine_threadsafe(message_queue.put(envelope), _loop)


def mqtt_thread_func():
    client = mqtt.Client(client_id="ws_bridge")
    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_message = on_mqtt_message
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except Exception as e:
            log.error("Nie można połączyć z MQTT: %s, retry za 3s", e)
            time.sleep(3)
    client.loop_forever()


async def ws_handler(websocket):
    connected_clients.add(websocket)
    log.info("Godot połączono: %s, łącznie: %d", websocket.remote_address, len(connected_clients))
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        log.info("Godot rozłączono, łącznie: %d", len(connected_clients))


async def broadcast_loop():
    while True:
        msg = await message_queue.get()
        dead = set()
        for ws in list(connected_clients):
            try:
                await ws.send(msg)
            except Exception:
                dead.add(ws)
        for ws in dead:
            connected_clients.discard(ws)


async def main():
    global _loop, message_queue
    _loop = asyncio.get_running_loop()
    message_queue = asyncio.Queue(maxsize=1000)

    t = threading.Thread(target=mqtt_thread_func, daemon=True)
    t.start()

    log.info("WebSocket serwer startuje na ws://%s:%d", WS_HOST, WS_PORT)
    asyncio.create_task(broadcast_loop())
    async with websockets.serve(ws_handler, WS_HOST, WS_PORT):
        log.info("Bridge gotowy. Czekam na połączenia Godot...")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
