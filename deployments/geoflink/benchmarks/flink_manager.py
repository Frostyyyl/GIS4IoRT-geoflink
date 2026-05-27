import json
import sys
import time

import requests

# Benchmark query configuration

# --- API CONFIGURATION ---
API_BASE_URL = "http://localhost:8000/geoflink"
HEADERS = {"Content-Type": "application/json"}

# --- FLINK JOB TEMPLATES ---

TEMPLATE_GEOFENCE = {
    "type": "GEOFENCE",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "range": 0.00000000001,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

TEMPLATE_SENSOR = {
    "type": "SENSOR",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "sensorTopicName": "sensor_proximity",
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

TEMPLATE_COLLISION = {
    "type": "COLLISION",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "multi_gps_fix",
    "collisionThreshold": 6,
    "robotStateTtlMillis": 5000,
    "robotAlertCooldownMillis": 0,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342
}

# --- SCENARIO DEFINITIONS ---
SCENARIO_RULES = {
    # Iteration 1-3: Geofence Monitoring
    1: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },
    2: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },
    3: {
        "geofences": [
            {"robot_id": "follower", "zone_id": "1 MONT"},
            {"robot_id": "leader",   "zone_id": "1 MONT"}
        ]
    },

    # Iteration 4-6: Sensor Proximity
    4: {
        "sensors": [
            {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
        ],
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    5: {
        "sensors": [
            {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
        ],
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    6: {
        "sensors": [
            {"sensor_id": "1", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "2", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "3", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "4", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "5", "radius": 3.0, "humidity_threshold": 80.0},
            {"sensor_id": "6", "radius": 3.0, "humidity_threshold": 80.0}
        ],
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },

    # Iteration 7-9: Collision Detection
    7: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    8: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    },
    9: {
        "robots": [
            {"robot_id": "follower"},
            {"robot_id": "leader"}
        ]
    }
}

DEFAULT_ROS_SETTINGS = {
    "bag_file": "/app/data/leader_follower/leader_follower.db3",
    "topics": "/leader/gps/fix /follower/gps/fix"
}

# Map assigning config type and ROS settings to iteration
TEST_CONFIGS_MAP = {
    1: {
        **TEMPLATE_GEOFENCE,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    2: {
        **TEMPLATE_GEOFENCE,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    3: {
        **TEMPLATE_GEOFENCE,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    4: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    5: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    6: {
        **TEMPLATE_SENSOR,
        "ros_settings": DEFAULT_ROS_SETTINGS
    },
    7: {
        **TEMPLATE_COLLISION,
        "ros_settings": {
            # Override for collision scenarios
            "bag_file": "/app/data/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    8: {
        **TEMPLATE_COLLISION,
        "ros_settings": {
            "bag_file": "/app/data/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    },
    9: {
        **TEMPLATE_COLLISION,
        "ros_settings": {
            "bag_file": "/app/data/leader_inv_follower/leader_inv_follower.db3",
            "topics": "/leader/gps/fix /follower/gps/fix"
        }
    }
}

# Push Geofence rules to the running job
def configure_geofence_rules(config_name, iteration):
    rules = SCENARIO_RULES.get(iteration, {}).get("geofences", [])

    print(f"   [API] Uploading {len(rules)} GEOFENCE rules...")

    for i, rule in enumerate(rules):
        payload = {
            "config_name": config_name,
            "robot_id": rule["robot_id"],
            "zone_id": rule["zone_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/geofence", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [{i+1}] Error: {resp.text}")
        except Exception as e:
            print(f"      [{i+1}] Critical: {e}")

# Configure sensor thresholds and assign robots to sensor monitoring
def configure_sensor_rules(config_name, iteration):
    data = SCENARIO_RULES.get(iteration, {})
    sensors = data.get("sensors", [])
    robots = data.get("robots", [])

    print(f"   [API] SENSOR Configuration: {len(sensors)} sensors, {len(robots)} robots...")

    # Define Sensors
    for i, s in enumerate(sensors):
        payload = {
            "config_name": config_name,
            "sensor_id": s["sensor_id"],
            "radius": s["radius"],
            "humidity_threshold": s["humidity_threshold"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/sensor", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [Sensor {s['sensor_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [Sensor {s['sensor_id']}] Critical: {e}")

    # Register Robots for Sensor tracking
    for i, r in enumerate(robots):
        payload = {
            "config_name": config_name,
            "robot_id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/sensor/robot", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [Robot {r['robot_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [Robot {r['robot_id']}] Critical: {e}")

# Register robots for collision detection monitoring
def configure_collision_rules(config_name, iteration):
    robots = SCENARIO_RULES.get(iteration, {}).get("robots", [])

    print(f"   [API] Uploading {len(robots)} robots to COLLISION...")

    for i, r in enumerate(robots):
        payload = {
            "config_name": config_name,
            "robot_id": r["robot_id"]
        }
        try:
            resp = requests.post(f"{API_BASE_URL}/collision", json=payload, headers=HEADERS)
            if resp.status_code != 200:
                print(f"      [{r['robot_id']}] Error: {resp.text}")
        except Exception as e:
            print(f"      [{r['robot_id']}] Critical: {e}")


def get_unique_name(iteration_number, config_type):
    return f"iter_{iteration_number:03d}_{config_type}"

# Generate dynamic metadata (Kafka topics, ROS commands) based on iteration config.
def get_test_metadata(iteration_number):
    config = TEST_CONFIGS_MAP.get(iteration_number)
    if not config:
        return None

    # Derive Output Topic from unique job name
    unique_name = get_unique_name(iteration_number, config['type'])
    output_topic = f"output_{unique_name}"

    # Build ROS2 bag play command
    ros_settings = config.get("ros_settings", DEFAULT_ROS_SETTINGS)
    bag_file = ros_settings["bag_file"]
    topics = ros_settings["topics"]

    ros_cmd = (
        "source /opt/ros/jazzy/setup.bash && "
        f"ros2 bag play {bag_file} --rate 1.0 "
        f"--topics {topics}"
    )

    return {
        "config_name": unique_name,
        "output_topic": output_topic,
        "ros_cmd": ros_cmd,
        "config_type": config['type']
    }

# Teardown existing job to ensure clean state
def delete_config_if_exists(config_name):
    url = f"{API_BASE_URL}/config/{config_name}"
    try:
        requests.delete(url)
    except Exception:
        pass
    time.sleep(1)

# Submit job configuration to Flink engine
def create_config(payload):
    url = f"{API_BASE_URL}/config"
    try:
        print(f"   [API] Registering Job: {payload['name']} (Type: {payload['type']})...")
        response = requests.post(url, json=payload, headers=HEADERS)
        if response.status_code == 200:
            return True
        elif response.status_code == 409:
            print("   [API ERROR] Config conflict (already exists).")
            return False
        else:
            print(f"   [API ERROR] {response.text}")
            return False
    except Exception as e:
        print(f"   [API CRITICAL] {e}")
        return False


def deploy_configuration(iteration_number):
    print(f"\n   [FLINK MANAGER] Configuration for iteration #{iteration_number}")

    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if not config_payload:
        print(f"   [WARN] No configuration for iteration {iteration_number}.")
        return None

    # Orchestrate job lifecycle: Clean -> Create -> Configure Rules
    unique_name = get_unique_name(iteration_number, config_payload['type'])
    payload_to_send = config_payload.copy()
    payload_to_send['name'] = unique_name

    delete_config_if_exists(unique_name)

    if not create_config(payload_to_send):
        raise RuntimeError("Flink Config Deployment Failed")

    print("   [FLINK MANAGER] Waiting 5s for Flink engine start...")
    time.sleep(5)

    job_type = config_payload['type']

    if job_type == 'GEOFENCE':
        configure_geofence_rules(unique_name, iteration_number)
    elif job_type == 'SENSOR':
        configure_sensor_rules(unique_name, iteration_number)
    elif job_type == 'COLLISION':
        configure_collision_rules(unique_name, iteration_number)

    return unique_name


def cleanup_job(iteration_number):
    config_payload = TEST_CONFIGS_MAP.get(iteration_number)
    if config_payload:
        unique_name = get_unique_name(iteration_number, config_payload['type'])
        delete_config_if_exists(unique_name)