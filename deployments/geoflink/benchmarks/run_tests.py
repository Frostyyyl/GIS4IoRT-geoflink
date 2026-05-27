import json
import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime

from kafka import KafkaConsumer

import flink_manager


# Runs GeoFlink architecture benchmarks.
# Benchmark query configuration lives in flink_manager.py.
# kafka_sensor_producer.py is used for telemetry logging and sensor signal generation.

# --- CONFIGURATION ---
ITERATIONS = 1
START_ITERATION = 1  # index of the first setup scenario to execute
OUTPUT_DIR = "test_results"
KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "multi_gps_fix"  # must match the topic name configured in the Kafka bridge component
SENSOR_TOPIC = "sensor_proximity"

# Docker & Script Paths
DOCKER_CONTAINER = "ros_bridge"
SENSOR_GENERATOR_SCRIPT = "kafka_sensor_producer.py"

# Global process handles for cleanup
current_sensor_process = None
current_collector = None


class DataCollector(threading.Thread):
    def __init__(self, test_id, target_topic, save_dir):
        super().__init__()
        self.test_id = test_id
        self.target_topic = target_topic
        self.save_dir = save_dir
        self.running = True
        self.collected_data = []
        self.group_id = f"collector_{test_id}_{random.randint(10000, 99999)}"
        
        # Daemonize to ensure thread dies if main process exits abruptly
        self.daemon = True

    def run(self):
        try:
            print(f"      [KAFKA] Listening on topic: {self.target_topic}")
            consumer = KafkaConsumer(
                self.target_topic,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='latest',
                value_deserializer=safe_json_deserializer,
                group_id=self.group_id,
                consumer_timeout_ms=1000
            )
            print(f"      [KAFKA] Collector connected (Group: {self.group_id})")

            while self.running:
                msg_pack = consumer.poll(timeout_ms=500)
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        record = msg.value
                        record['ts_received'] = int(time.time() * 1000)
                        self.collected_data.append(record)

            consumer.close()

        except Exception as e:
            print(f"      [ERROR] Collector error: {e}")

    def stop_and_save(self):
        self.running = False
        if self.is_alive():
            self.join(timeout=2.0)

        filename = os.path.join(self.save_dir, f"results_{self.test_id}.json")

        if not self.collected_data:
            print(f"   [IO] No data to save for {self.test_id} (Test aborted?)")
            return

        report = {
            "test_id": self.test_id,
            "topic": self.target_topic,
            "timestamp": datetime.now().isoformat(),
            "records": len(self.collected_data),
            "data": self.collected_data
        }

        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"   [IO] Saved: {filename} (Records: {len(self.collected_data)})")
        except Exception as e:
            print(f"   [IO] File write error: {e}")


def safe_json_deserializer(x):
    """Safely decode JSON from Kafka bytes."""
    try:
        text = x.decode('utf-8').strip()
        if not text:
            return None
        return json.loads(text)
    except Exception:
        print("[WARN] Invalid JSON:", x)
        return None


def cleanup():
    """Graceful shutdown of threads and subprocesses."""
    global current_sensor_process, current_collector

    print("\n[CLEANUP] Cleaning up processes...")

    if current_sensor_process:
        if current_sensor_process.poll() is None:
            print("   -> Sending SIGINT to generator (Flush)...")
            current_sensor_process.terminate()
            try:
                current_sensor_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("   -> Process unresponsive, killing!")
                current_sensor_process.kill()

    if current_collector and current_collector.is_alive():
        print("   -> Stopping Collector...")
        current_collector.running = False
        current_collector.stop_and_save()

def wait_for_kafka_topic(topic, timeout=60):
    print(f"   [CHECK] Ensuring Kafka topic '{topic}' is fully initialized...")
    start = time.time()

    try:
        subprocess.run(
            ["docker", "exec", "kafka-broker", "kafka-topics", "--bootstrap-server", "broker:29092",
             "--create", "--if-not-exists", "--topic", topic],
            capture_output=True, text=True
        )
    except:
        pass

    while True:
        result = subprocess.run(
            ["docker", "exec", "kafka-broker", "kafka-topics", "--bootstrap-server", "broker:29092",
             "--describe", "--topic", topic],
            capture_output=True, text=True
        )

        if "Partition:" in result.stdout and "Leader:" in result.stdout:
            print(f"   [CHECK] Topic '{topic}' is ready and has partitions assigned.")
            return True

        if time.time() - start > timeout:
            print(f"   [ERROR] Timeout waiting for topic '{topic}'.")
            print(f"   [DEBUG] Kafka describe output: {result.stdout}")
            return False

        time.sleep(2)
        

def run_tests():
    global current_sensor_process, current_collector

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"=== STARTING TESTS (Ctrl+C to abort all) ===")

    for i in range(START_ITERATION, ITERATIONS + START_ITERATION):
        
        # Retrieve dynamic config from manager
        meta = flink_manager.get_test_metadata(i)
        
        if not meta:
            print(f"[SKIP] No configuration for iteration {i}")
            continue

        test_id = meta['config_name']
        test_type = meta['config_type']
        dynamic_topic = meta['output_topic']
        dynamic_ros_cmd = meta['ros_cmd']

        # Setup iteration directory
        test_run_dir = os.path.join(OUTPUT_DIR, test_id)
        if not os.path.exists(test_run_dir):
            os.makedirs(test_run_dir)

        robot_log_path = os.path.join(test_run_dir, "robot_raw.jsonl")
        sensor_log_path = os.path.join(test_run_dir, "sensor_out.jsonl")

        print(f"\n--- RUN {i}/{ITERATIONS} [{test_id}] ---")

        current_sensor_process = None
        current_collector = None

        try:
            wait_for_kafka_topic(INPUT_TOPIC)
            wait_for_kafka_topic(SENSOR_TOPIC)

            # Deploy Flink Job
            try:
                deployed_name = flink_manager.deploy_configuration(i)
                if not deployed_name:
                    raise RuntimeError("Deployment returned empty name")
            except Exception as e:
                print(f"   [CRITICAL] Cluster configuration error: {e}")
                cleanup()
                continue

            # Start Data Collector (Thread)
            print(f"   [1/3] Starting Data Collector...")
            current_collector = DataCollector(test_id, dynamic_topic, test_run_dir)
            current_collector.start()

            time.sleep(2.0)

            # Start Sensor Generator (Subprocess)
            print(f"   [2/3] Starting Sensor Generator...")
            current_sensor_process = subprocess.Popen([
                sys.executable,
                SENSOR_GENERATOR_SCRIPT,
                "--robot_log", robot_log_path,
                "--sensor_log", sensor_log_path,
                "--test_type", test_type,
                "--robot_topic", INPUT_TOPIC,
                "--sensor_topic", SENSOR_TOPIC,
            ])

            time.sleep(2.0)

            # Start Simulation (Docker Exec)
            print(f"   [3/3] Starting Robot...")
            subprocess.run([
                "docker", "exec", DOCKER_CONTAINER,
                "bash", "-c", dynamic_ros_cmd
            ], check=True, stdout=subprocess.DEVNULL)

            print("         -> Robot finished route. Waiting for data ingestion...")
            time.sleep(5.0)

            cleanup()

        except subprocess.CalledProcessError as e:
            print(f"   [ERROR] Subprocess command error: {e}")
            cleanup()

        finally:
            print(f"   [FLINK] Cleaning up Job after iteration {i}...")
            flink_manager.cleanup_job(i)
            cleanup()

    print("\n=== TESTS FINISHED ===")

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        print("\n\n!!! CTRL+C DETECTED - EMERGENCY STOP !!!")
        cleanup()
        sys.exit(0)