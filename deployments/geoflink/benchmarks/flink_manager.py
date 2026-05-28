"""
Flink Manager module for GIS4IoRT benchmarks.
This module provides functions to deploy Flink job configurations, manage geofence rules,
and handle cleanup after benchmark runs.
"""

import time
import requests

from config import (
    API_BASE_URL,
    HEADERS,
    SCENARIO_RULES,
    TEST_CONFIGS_MAP,
)


def configure_geofence_rules(config_name, iteration):
    """Upload geofence rules for the given iteration to the Flink manager via API."""
    rules = SCENARIO_RULES.get(iteration, {}).get("GEOFENCE", [])

    print(f"   [API] Uploading {len(rules)} GEOFENCE rules...")

    for i, rule in enumerate(rules):
        payload = {
            "config_name": config_name,
            "robot_id": rule["robot_id"],
            "zone_id": rule["zone_id"],
        }

        try:
            resp = requests.post(
                f"{API_BASE_URL}/geofence",
                json=payload,
                headers=HEADERS,
            )

            if resp.status_code != 200:
                print(f"      [{i+1}] Error: {resp.text}")

        except Exception as e:
            print(f"      [{i+1}] Critical: {e}")


def get_unique_name(iteration_number, config_type):
    """Generate a unique name for the Flink job based on iteration and config type."""
    return f"iter_{iteration_number:03d}_{config_type}"


def get_test_metadata(iteration_number):
    """Derive dynamic metadata for the test iteration based on the configuration map."""
    config = TEST_CONFIGS_MAP.get(iteration_number)

    if not config:
        return None

    unique_name = get_unique_name(iteration_number, config["type"])

    output_topic = f"output_{unique_name}"

    return {
        "config_name": unique_name,
        "output_topic": output_topic,
        "config_type": config["type"],
    }


def delete_config_if_exists(config_name):
    """Delete existing configuration with the same name to ensure clean deployment."""
    url = f"{API_BASE_URL}/config/{config_name}"

    try:
        requests.delete(url)
    except Exception:
        pass

    time.sleep(1)


def create_config(payload):
    """Create a new Flink job configuration via API."""
    url = f"{API_BASE_URL}/config"

    try:
        print(
            f"   [API] Registering Job: "
            f"{payload['name']} "
            f"(Type: {payload['type']})..."
        )

        response = requests.post(
            url,
            json=payload,
            headers=HEADERS,
        )

        if response.status_code == 200:
            return True

        if response.status_code == 409:
            print("   [API ERROR] Config conflict (already exists).")
            return False

        print(f"   [API ERROR] {response.text}")
        return False

    except Exception as e:
        print(f"   [API CRITICAL] {e}")
        return False


def deploy_configuration(iteration_number):
    """Main method to deploy the Flink job configuration for a given iteration."""
    print(f"\n   [FLINK MANAGER] " f"Configuration for iteration #{iteration_number}")

    config_payload = TEST_CONFIGS_MAP.get(iteration_number)

    if not config_payload:
        print(f"   [WARN] No configuration for iteration {iteration_number}.")
        return None

    unique_name = get_unique_name(iteration_number, config_payload["type"])

    payload_to_send = config_payload.copy()
    payload_to_send["name"] = unique_name

    delete_config_if_exists(unique_name)

    if not create_config(payload_to_send):
        raise RuntimeError("Flink Config Deployment Failed")

    print("   [FLINK MANAGER] Waiting 5s for Flink engine start...")
    time.sleep(5)

    job_type = config_payload["type"]

    if job_type == "GEOFENCE":
        configure_geofence_rules(unique_name, iteration_number)

    elif job_type == "SENSOR":
        configure_sensor_rules(unique_name, iteration_number)

    elif job_type == "COLLISION":
        configure_collision_rules(unique_name, iteration_number)

    return unique_name


def cleanup_job(iteration_number):
    """Cleanup Flink job configuration for the given iteration."""
    config_payload = TEST_CONFIGS_MAP.get(iteration_number)

    if config_payload:
        unique_name = get_unique_name(iteration_number, config_payload["type"])

        delete_config_if_exists(unique_name)
