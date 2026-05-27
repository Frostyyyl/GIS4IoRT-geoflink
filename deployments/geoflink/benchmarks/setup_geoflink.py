"""
Setup script to register zones and robots in Geoflink.
Run this once before running tests: python setup_geoflink.py
"""

import requests
import json

API_BASE_URL = "http://localhost:8000/geoflink"
HEADERS = {"Content-Type": "application/json"}

ZONES = [
    {
        "id": "1 MONT",
        "geo": "0103000020E61000000100000005000000C081182C28780B4068436B9D732B4740803F61E91B770B4074FEB9DD6E2B474080629C0F4C770B4010DE9EC4602B474000977541CD780B403C520AF8692B4740C081182C28780B4068436B9D732B4740"
    }
]

def register_zones():
    print("=== ZONE REGISTRATION ===")
    
    for zone in ZONES:
        print(f"\nRegistering zone: {zone['id']}")
        
        payload = {
            "id": zone["id"],
            "geo": zone["geo"]
        }
        
        try:
            resp = requests.post(f"{API_BASE_URL}/zones", json=payload, headers=HEADERS)
            
            if resp.status_code in [200, 201]:
                print(f"✓ Zone '{zone['id']}' registered successfully")
            elif resp.status_code == 409:
                print(f"✓ Zone '{zone['id']}' already exists")
            else:
                print(f"✗ Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"✗ Critical error: {e}")

def register_robots():
    print("\n=== ROBOT REGISTRATION ===")
    
    robots = ["leader", "follower"]
    
    for robot_id in robots:
        print(f"\nRegistering robot {robot_id}...")
        
        payload = {"id": robot_id}
        
        try:

            resp = requests.post(f"{API_BASE_URL}/robots", json=payload, headers=HEADERS)
            
            if resp.status_code in [200, 201]:
                print(f"✓ Robot {robot_id} registered")
            elif resp.status_code == 409:
                print(f"✓ Robot {robot_id} already exists")
            else:
                print(f"✗ Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"✗ Critical: {e}")

if __name__ == "__main__":
    try:
        requests.get("http://localhost:8000/docs", timeout=1)
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Unable to connect with {API_BASE_URL}. Is the app running?")
        exit(1)

    register_zones()
    register_robots()