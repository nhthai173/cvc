import paho.mqtt.client as mqtt_client
import json
import time
import random
from datetime import datetime, timezone

BROKER = 'nhatvietindustry.ddns.net'
PORT = 18583
CLIENT_ID = 'mqtt_test_subscriber'
USERNAME = 'server'
PASSWORD = 'server@123'
TOPIC = 'raw/test'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(TOPIC)
        print(f"Subscribed to topic: {TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")

client = mqtt_client.Client(CLIENT_ID)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.connect(BROKER, PORT)
client.loop_start()

while True:
    time.sleep(1)
    payload = {"ts": int(time.time() * 1000), "value": random.randint(0, 100)}
    result = client.publish(TOPIC, json.dumps(payload))
    status = result[0]
    if status == 0:
        print(f"Sent `{payload}` to topic `{TOPIC}`")
    else:
        print(f"Failed to send message to topic {TOPIC}")