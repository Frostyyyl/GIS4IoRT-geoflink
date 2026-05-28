"""
Configuration module for GIS4IoRT GeoFlink benchmarks.
This module centralizes all configuration parameters, including API endpoints, Kafka settings,
Flink job templates, and scenario definitions. It serves as a single source of truth for all
benchmark-related settings, making it easier to manage and modify test parameters without
having to change the core logic of the benchmark scripts.
"""

# --- KAFKA CONFIGURATION ---

KAFKA_BROKER = "broker:9092"
KAFKA_POLL_TIMEOUT_MS = 500
KAFKA_CONSUMER_TIMEOUT_MS = 1000

# --- API CONFIGURATION ---

API_BASE_URL = "http://localhost:8000/geoflink"
HEADERS = {"Content-Type": "application/json"}

# --- FLINK JOB TEMPLATES ---

TEMPLATE_GEOFENCE = {
    "type": "GEOFENCE",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "ros2.fleet.gnss",
    "range": 0.00000000001,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342,
}

# --- SCENARIO DEFINITIONS ---

NUM_ROBOTS = 10

ROBOTS = [f"robot_{i}" for i in range(1, NUM_ROBOTS + 1)]

ZONES = [
    {
        "id": "1 MONT",
        "geo": (
            "0103000000010000000600000005cbc7a475d83d400ba139a1c66d4340"
            "d7dd3cd521e33d40cb38fc242d734340db98e83ddfee3d4042f79b3f5c"
            "6d4340d7dd3cd521e33d4054e41071736a43408f5951de22db3d40bd1a"
            "a034d46b434005cbc7a475d83d400ba139a1c66d4340"
        ),
    }
]

SCENARIO_RULES = {
    1: {
        "GEOFENCE": [
            {"robot_id": id, "zone_id": ZONES[0]["id"]}
            for id in ROBOTS
        ]
    }
}

TEST_CONFIGS_MAP = {}

for i in SCENARIO_RULES.keys():
    if SCENARIO_RULES[i].get("GEOFENCE"):
        TEST_CONFIGS_MAP[i] = {**TEMPLATE_GEOFENCE}

# --- OTHER CONFIGURATION ---

COLLECTOR_RUNTIME_SECONDS = 60
ITERATIONS = 1
START_ITERATION = 1
OUTPUT_DIR = "test_results"
