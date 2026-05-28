"""
Setup script to register zones and robots in Geoflink.
Run this once before running tests: python setup_geoflink.py
"""

import requests
import json

from config import (
    API_BASE_URL,
    HEADERS,
    ZONES,
    ROBOTS,
)


def post_resource(endpoint, payload, resource_name):
    """Generic POST helper for GeoFlink resources."""

    url = f"{API_BASE_URL}/{endpoint}"

    try:
        response = requests.post(
            url,
            json=payload,
            headers=HEADERS,
        )

        if response.status_code in (200, 201):
            print(f"   ✓ {resource_name} registered")
            return True
        if response.status_code == 409:
            print(f"   ✓ {resource_name} already exists")
            return True

        print(f"   ✗ Registration failed " f"({response.status_code})")
        print(f"     -> {response.text}")

        return False

    except Exception as e:
        print(f"   ✗ Critical error: {e}")
        return False


def register_zones():
    """Register all predefined geofence zones."""

    print("\n=== ZONE REGISTRATION ===")

    for zone in ZONES:

        print(f"\n[ZONE] {zone['id']}")

        payload = {
            "id": zone["id"],
            "geo": zone["geo"],
        }

        post_resource(
            endpoint="zones",
            payload=payload,
            resource_name=f"Zone '{zone['id']}'",
        )


def register_robots():
    """Register benchmark robots."""

    print("\n=== ROBOT REGISTRATION ===")

    for robot_id in ROBOTS:

        print(f"\n[ROBOT] {robot_id}")

        payload = {
            "id": robot_id,
        }

        post_resource(
            endpoint="robots",
            payload=payload,
            resource_name=f"Robot '{robot_id}'",
        )


if __name__ == "__main__":
    try:
        requests.get("http://localhost:8000/docs", timeout=1)
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Unable to connect with {API_BASE_URL}. Is the app running?")
        exit(1)

    register_zones()
    register_robots()
