import sqlite3
import logging
from typing import List, Dict, Optional
import json
from .schemas import RobotEntry, ZoneEntry
from app.config import settings

DB_NAME = settings.GEOFLINK_DB_NAME
logger = logging.getLogger("uvicorn.info")

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    try:
        with get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS robots (
                    id TEXT PRIMARY KEY,
                    status TEXT
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS zones (
                    id TEXT PRIMARY KEY,
                    geo TEXT
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS configurations (
                    name TEXT PRIMARY KEY, 
                    config_json TEXT,
                    flink_job_id TEXT,
                    control_topic TEXT,
                    output_topic TEXT  
                );
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS geofence_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    robot_id TEXT,
                    zone_id TEXT,
                    config_name TEXT,           
                    FOREIGN KEY(config_name) REFERENCES configurations(name) ON DELETE CASCADE,
                    UNIQUE(robot_id, zone_id, config_name)
                );
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS sensor_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_id TEXT NOT NULL,
                    config_name TEXT NOT NULL,
                    radius REAL NOT NULL,
                    humidity_threshold REAL NOT NULL,
                    FOREIGN KEY(config_name) REFERENCES configurations(name) ON DELETE CASCADE,
                    UNIQUE(sensor_id, config_name)
                );
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS robot_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    robot_id TEXT NOT NULL,
                    config_name TEXT NOT NULL,
                    FOREIGN KEY(config_name) REFERENCES configurations(name) ON DELETE CASCADE,
                    UNIQUE(robot_id, config_name)
                );
            ''')




            logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise



def upsert_robot(robot: RobotEntry):
    with get_connection() as conn:
        conn.execute('''
            INSERT INTO robots (id, status) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET status=excluded.status
        ''', (robot.id, robot.status))

def upsert_zone(zone: ZoneEntry):
    with get_connection() as conn:
        conn.execute('''
            INSERT INTO zones (id, geo) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET geo=excluded.geo
        ''', (zone.id, zone.geo))


def get_all_robots() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM robots").fetchall()
        return [dict(row) for row in rows]

def get_all_zones() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM zones").fetchall()
        return [dict(row) for row in rows]

def get_robot(robot_id: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM robots WHERE id = ?", (robot_id,)).fetchone()
        return dict(row) if row else None


def get_zone(zone_id: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM zones WHERE id = ?", (zone_id,)).fetchone()
        return dict(row) if row else None


def delete_robot(robot_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM geofence_assignments WHERE robot_id = ?", (robot_id,))
        conn.execute("DELETE FROM robots WHERE id = ?", (robot_id,))

def delete_zone(zone_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM geofence_assignments WHERE zone_id = ?", (zone_id,))
        conn.execute("DELETE FROM zones WHERE id = ?", (zone_id,))



#---------------------------------------
# GEOFLINK-specific functions

# Configuration operations

def create_config(name: str, config_data: dict):
    control_topic = f"control_{name}"
    output_topic = f"output_{name}"
    
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO configurations 
            (name, config_json, control_topic, output_topic) 
            VALUES (?, ?, ?, ?)
            """,
            (name, json.dumps(config_data), control_topic, output_topic)
        )

def get_config_state(name: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM configurations WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None

def update_job_status(name: str, job_id: Optional[str]):
    with get_connection() as conn:
        conn.execute("UPDATE configurations SET flink_job_id = ? WHERE name = ?", (job_id, name))

def get_all_configs() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM configurations").fetchall()
        return [dict(row) for row in rows]

def get_configs_formatted() -> List[dict]:
    raw_configs = get_all_configs() 
    formatted_list = []

    for row in raw_configs:
        try:
            details = json.loads(row['config_json']) if row['config_json'] else {}
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Corrupted JSON in config '{row['name']}'")
            details = {}
        
        status = "RUNNING" if row['flink_job_id'] else "STOPPED"

        formatted_list.append({
            "name": row['name'],
            "status": status,
            "job_id": row['flink_job_id'], 
            "details": details, 
            "control_topic": row["control_topic"],
            "output_topic": row["output_topic"] 
        })

    return formatted_list

def delete_config(config_name: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM configurations WHERE name = ?", (config_name,))


# Job assignment operations

def count_active_assignments(config_name: str) -> int:
    with get_connection() as conn:
        q1_count = conn.execute(
            "SELECT count(*) FROM geofence_assignments WHERE config_name=?", 
            (config_name,)
        ).fetchone()[0]

        q2_s_count = conn.execute(
            "SELECT count(*) FROM sensor_assignments WHERE config_name=?", 
            (config_name,)
        ).fetchone()[0]

        q2_r_count = conn.execute(
            "SELECT count(*) FROM robot_assignments WHERE config_name=?", 
            (config_name,)
        ).fetchone()[0]

        return q1_count + q2_s_count + q2_r_count


def add_geofence_assignment(robot_id: str, zone_id: str, config_name: str):
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO geofence_assignments (robot_id, zone_id, config_name) VALUES (?, ?, ?)",
                (robot_id, zone_id, config_name)
            )
    except sqlite3.IntegrityError:
        pass 
    except Exception as e:
        logger.error(f"Error adding assignment: {e}")
        raise

def remove_geofence_assignment(robot_id: str, zone_id: str, config_name: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM geofence_assignments WHERE robot_id = ? AND zone_id = ? AND config_name = ?",
            (robot_id, zone_id, config_name)
        )        
        return cursor.rowcount > 0

def get_geofence_assignments(config_name: str) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT robot_id, zone_id FROM geofence_assignments WHERE config_name = ?", 
            (config_name,)
        ).fetchall()
        
        return [dict(row) for row in rows]


def get_geofence_assignment(robot_id: str, zone_id: str, config_name: str) -> dict:

    with get_connection() as conn:
    
        query = """
            SELECT * FROM geofence_assignments 
            WHERE config_name=? 
            AND robot_id=? 
            AND zone_id=?
        """

        params = (config_name, robot_id, zone_id)
        row = conn.execute(query, params).fetchone()

        return dict(row) if row else None

# Sensor proximity

def add_sensor_assignment(sensor_id: str, radius: float, threshold: float, config_name: str):
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sensor_assignments 
                (sensor_id, config_name, radius, humidity_threshold) 
                VALUES (?, ?, ?, ?)
                """,
                (sensor_id, config_name, radius, threshold)
            )
    except sqlite3.IntegrityError:
        pass 
    except Exception as e:
        logger.error(f"Error adding sensor assignment: {e}")
        raise


def remove_sensor_assignment(sensor_id: str, config_name: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM sensor_assignments WHERE sensor_id = ? AND config_name = ?",
            (sensor_id, config_name)
        )        
        return cursor.rowcount > 0

def get_sensor_assignments(config_name: str) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT sensor_id, radius, humidity_threshold FROM sensor_assignments WHERE config_name = ?", 
            (config_name,)
        ).fetchall()
        
        return [dict(row) for row in rows]

def get_sensor_assignment(sensor_id: str, config_name: str) -> Optional[dict]:
    with get_connection() as conn:
        query = """
            SELECT * FROM sensor_assignments 
            WHERE config_name=? AND sensor_id=?
        """
        row = conn.execute(query, (config_name, sensor_id)).fetchone()
        return dict(row) if row else None


# Robot whitelist

def add_robot_assignment(robot_id: str, config_name: str):
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO robot_assignments (robot_id, config_name) VALUES (?, ?)",
                (robot_id, config_name)
            )
    except Exception as e:
        logger.error(f"Error adding robot assignment: {e}")
        raise

def remove_robot_assignment(robot_id: str, config_name: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM robot_assignments WHERE robot_id = ? AND config_name = ?",
            (robot_id, config_name)
        )        
        return cursor.rowcount > 0

def get_robot_assignments(config_name: str) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT robot_id FROM robot_assignments WHERE config_name = ?", 
            (config_name,)
        ).fetchall()
        return [dict(row) for row in rows]

def get_robot_assignment(robot_id: str, config_name: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM robot_assignments WHERE config_name=? AND robot_id=?", 
            (config_name, robot_id)
        ).fetchone()
        return dict(row) if row else None


def find_all_jobs_for_robot(robot_id: str) -> List[dict]:
    assignments = []
    
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        
        rows_geo = conn.execute(
            "SELECT config_name, zone_id FROM geofence_assignments WHERE robot_id=?", 
            (robot_id,)
        ).fetchall()
        
        for row in rows_geo:
            assignments.append({
                "type": "GEOFENCE",
                "config_name": row['config_name'],
                "robot_id": robot_id,
                "zone_id": row['zone_id']
            })

        rows_sensor = conn.execute(
            "SELECT config_name FROM robot_assignments WHERE robot_id=?", 
            (robot_id,)
        ).fetchall()
        
        for row in rows_sensor:
            assignments.append({
                "type": "SENSOR", 
                "config_name": row['config_name'],
                "robot_id": robot_id,
                "zone_id": None 
            })
            
    return assignments

def find_all_jobs_for_zone(zone_id: str) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT config_name, robot_id FROM geofence_assignments WHERE zone_id=?", 
            (zone_id,)
        ).fetchall()
        
        return [{
            "type": "GEOFENCE",
            "config_name": row['config_name'],
            "robot_id": row['robot_id'],
            "zone_id": zone_id
        } for row in rows]




