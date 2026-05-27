import json
import time
import sys
import os
import math
import signal
from decimal import Decimal, getcontext
from kafka import KafkaConsumer, KafkaProducer
import argparse

# IoT Sensor Proximity Simulator and Heartbeat Generator.
# This script simulates a network of environmental sensors (e.g., humidity) that respond to robot proximity.
# It also logs robot telemetry records


# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
CONFIG_FILE = os.getenv('SENSOR_CONFIG', r"../docker/data/sensor_config_updated.json") 
TARGET_ROBOT_ID = "follower" # id of the tracked robot - necessary for SWITCH behaviour

TARGET_FREQUENCY_HZ = 2.0 
MIN_INTERVAL_MS = 1000.0 / TARGET_FREQUENCY_HZ 

# Precision for Decimal
getcontext().prec = 50

class ProductionSensor:
    def __init__(self, robot_log_path, sensor_log_path, test_type,input_topic, sensor_topic):
        print(f"[INFO] Starting Sensors (Exact Match + Heartbeat {TARGET_FREQUENCY_HZ}Hz)")
        self.running = True
        
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        
        self.robot_log_file = open(robot_log_path, "a", encoding="utf-8")
        self.test_type = test_type
        if self.test_type == 'SENSOR':
            self.sensor_log_file = open(sensor_log_path, "a", encoding="utf-8")
        
        self.topic_in_robot = input_topic
        self.topic_out_sensor = sensor_topic

        def decimal_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj) 
            raise TypeError

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=decimal_serializer).encode('utf-8')
        )

        self.consumer = KafkaConsumer(
            self.topic_in_robot,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'), parse_float=Decimal),
            auto_offset_reset='latest', 
            group_id=f"sensor_decimal_freq_{int(time.time())}"
        )

        self.sensors = self.load_sensors()
        print(f"[INFO] Loaded {len(self.sensors)} sensors. Waiting for data...")

    def handle_exit(self, signum, frame):
        print("\n[INFO] Shutting down...")
        self.running = False

    def load_sensors(self):
        if not os.path.exists(CONFIG_FILE):
            print(f"[ERROR] File missing: {CONFIG_FILE}")
            sys.exit(1)
            
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f, parse_float=Decimal)
            
        for s in data:
            s['active'] = False         
            s['switched'] = False 
            s['prev_val'] = -1.0
            s['last_sent_ts'] = 0
            
            if 'switch_lat' in s:
                try:
                    s['switch_lat'] = Decimal(str(s['switch_lat']))
                    s['switch_lon'] = Decimal(str(s['switch_lon']))
                except: pass

            # Mapping behavior
            sid = s['sensor_id']
            if sid == 1: s['behavior'] = "ALWAYS_HIGH"
            elif sid == 2: s['behavior'] = "ALWAYS_LOW"
            elif sid == 3: s['behavior'] = "SWITCH_HIGH_LOW"
            elif sid == 4: s['behavior'] = "SWITCH_LOW_HIGH"
            elif sid == 5: s['behavior'] = "ALWAYS_HIGH"
            elif sid == 6: s['behavior'] = "ALWAYS_HIGH"
            else: s['behavior'] = "ALWAYS_LOW"
                
        return data

    def haversine(self, lat1, lon1, lat2, lon2):
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        R = 6371000
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def calculate_humidity(self, sensor):
        if "ALWAYS_HIGH" in sensor['behavior']: return 90.0
        if "ALWAYS_LOW" in sensor['behavior']: return 35.0
        
        is_switched = sensor['switched']
        if sensor['behavior'] == "SWITCH_HIGH_LOW":
            return 35.0 if is_switched else 90.0
        elif sensor['behavior'] == "SWITCH_LOW_HIGH":
            return 90.0 if is_switched else 35.0
        return 35.0

    def run(self):
        try:
            while self.running:
                msg_pack = self.consumer.poll(timeout_ms=500)
                if not msg_pack: continue

                for tp, messages in msg_pack.items():
                    for message in messages:
                        payload = message.value

                        self.robot_log_file.write(json.dumps(payload, default=str) + "\n")

                        if self.test_type != 'SENSOR' or payload.get('id') != TARGET_ROBOT_ID: continue 
                        
                        lat, lon, ts = payload['lat'], payload['lon'], payload['ts']

                        for s in self.sensors:
                            dist_to_center = self.haversine(s['lat'], s['lon'], lat, lon)
                            is_inside = dist_to_center <= float(s['radius'])
                            
                            if is_inside:
                                if not s['active']:
                                    s['active'] = True
                                    s['switched'] = False
                                    s['last_sent_ts'] = 0 
                                    print(f"[SENSOR {s['sensor_id']}] Entry")

                                if 'switch_lat' in s and not s['switched']:
                                    if lat == s['switch_lat'] and lon == s['switch_lon']:
                                        s['switched'] = True
                                        print(f"[EVENT] Sensor {s['sensor_id']} SWITCH (Exact Match)")
                            else:
                                if s['active']:
                                    s['active'] = False

                            val = self.calculate_humidity(s)
                            
                            value_changed = abs(val - s['prev_val']) > 0.1
                            
                            time_diff = ts - s['last_sent_ts']
                            timer_expired = time_diff >= MIN_INTERVAL_MS
                            
                            should_send = value_changed or timer_expired
                            
                            if should_send:
                                out = {
                                    "timestamp": ts,
                                    "sensor_id": s['sensor_id'],
                                    "position_x": s['lon'], 
                                    "position_y": s['lat'],
                                    "humidity": val
                                }
                                key = str(s['sensor_id']).encode('utf-8')
                                self.producer.send(self.topic_out_sensor, value=out, key=key)
                                self.sensor_log_file.write(json.dumps(out, default=str) + "\n")
                                s['prev_val'] = val
                                s['last_sent_ts'] = ts
                                
                                if value_changed:
                                    print(f">>> ID {s['sensor_id']}: {val} (Change)")

        except Exception as e:
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.robot_log_file.close()
            if self.test_type == 'SENSOR':
                self.sensor_log_file.close()
            try:
                self.producer.flush()
                self.producer.close()
                self.consumer.close()
            except: pass
            sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--robot_log", default="robots_raw.jsonl")
    parser.add_argument("--sensor_log", default="sensors_out.jsonl")
    parser.add_argument("--test_type", default="SENSOR")
    parser.add_argument("--robot_topic", default="multi_gps_fix")
    parser.add_argument("--sensor_topic", default="sensor_proximity")
    args = parser.parse_args()

    app = ProductionSensor(args.robot_log, args.sensor_log, args.test_type, args.robot_topic, args.sensor_topic)
    app.run()