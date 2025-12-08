import json
import os
import time
from datetime import datetime, timezone
from paho.mqtt import client as mqtt_client
from common.DataModel.db import PostgresDB
from common.config.settings import settings as cfg

def print_log(message):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}")

# MQTT configuration
BROKER = cfg.mqtt.broker
PORT = int(cfg.mqtt.port)
CLIENT_ID = cfg.mqtt.client_id
USERNAME = cfg.mqtt.username
PASSWORD = cfg.mqtt.password

# Topics to subscribe
TOPICS = ["raw/#", "gateway/status"]

# Global DB instance
db = PostgresDB()
db.connect()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print_log("Connected to MQTT Broker!")
        # Subscribe to all topics
        for topic in TOPICS:
            client.subscribe(topic)
            print_log(f"Subscribed to topic: {topic}")
    else:
        print_log(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        print_log(f"Received `{payload}` from `{topic}` topic")

        if topic == "gateway/status":
            store_gateway_status(payload)
        else:
            store_raw_data(topic, payload)
    except Exception as e:
        print_log(f"Error processing message: {e}")

def parse_json_data(data):
    json_data = None
    try:
        json_data = json.loads(data)
        if not isinstance(json_data, dict):
            raise(ValueError("Invalid JSON format"))
        timestamp = json_data.get('ts')
        if timestamp and isinstance(timestamp, int):
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            pg_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            return json_data, pg_timestamp
        return json_data, None
    except Exception as e:
        print_log(f"JSON decode error: {e}")
        return None, None


def store_raw_data(topic, data):
    try:
        json_data, pg_timestamp = parse_json_data(data)
        if json_data is None:
            raise(ValueError("Failed to parse JSON data"))
        if pg_timestamp is None:
            pg_timestamp = "now()"
        insert_query = """
        INSERT INTO raw_log (topic, data, ts) VALUES (%s, %s, %s)
        """
        db.execute_non_query(insert_query, (topic, data, pg_timestamp))
        print_log(f"Inserted raw data into database for topic: {topic}")
    except Exception as e:
        print_log(f"Error inserting raw data: {e}")


def store_gateway_status(data):
    try:
        status, ts = parse_json_data(data)
        if status is None:
            raise(ValueError("Invalid gateway status format"))
        if ts is None:
            ts = "now()"
        is_online = status.get('is_online', False)
        gwid = status.get('gwid', 'gateway1')
        insert_query = """
        INSERT INTO gateway_status (gwid, is_online, ts) VALUES (%s, %s, %s)
        """
        db.execute_non_query(insert_query, (gwid, is_online, ts))
        print_log("Inserted gateway status into database")
    except Exception as e:
        print_log(f"Error processing gateway status: {e}")

def start_mqtt_subscriber():
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, CLIENT_ID)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_start()

if __name__ == "__main__":
    start_mqtt_subscriber()
    try:
        while True:
            pass
    except Exception as e:
        print_log(f"An error occurred: {e}")
    finally:
        print_log("Shutting down...")
        db.close()