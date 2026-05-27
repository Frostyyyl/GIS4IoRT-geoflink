import logging
import json
from app.adapters.geoflink import database
from .consumer_manager import consumer_manager
from .kafka_service import kafka_service
from .flink_service import get_flink_service
from .schemas import ZoneCreate

logger = logging.getLogger("uvicorn.info")

# System State Recovery and Synchronization Service.
# This module is responsible for reconciling the local database state with the Apache Flink cluster 
# and Kafka infrastructure after an application restart


async def restore_application_state():
    configs = database.get_all_configs()
    flink_service = get_flink_service()
    
    if not kafka_service.producer:
        await kafka_service.start()

    logger.info(f"Starting System Recovery for {len(configs)} configurations...")
    restored_count = 0
    
    for config in configs:
        config_name = config["name"]
        saved_job_id = config["flink_job_id"]
        
        config_def = json.loads(config['config_json'])
        config_type = config_def.get('type', 'GEOFENCE')
        
        control_topic = config.get('control_topic') 
        output_topic = config.get("output_topic") or f"output_{config_name}"

        should_run = False
        reload_data = {} 
        
        assignments = [] 

        if config_type == 'GEOFENCE':
            assignments = database.get_geofence_assignments(config_name)
            if len(assignments) > 0:
                should_run = True
                reload_data['assignments'] = assignments
                
        elif config_type == 'SENSOR':
            sensors = database.get_sensor_assignments(config_name)
            robots = database.get_robot_assignments(config_name)
            
            reload_data['sensors'] = sensors
            reload_data['robots'] = robots
            
            if len(sensors) > 0 and len(robots) > 0:
                should_run = True
            else:
                logger.info(f"Config '{config_name}' (SENSOR) has partial rules - keeping asleep.")

        elif config_type == 'COLLISION':
            robots = database.get_robot_assignments(config_name)
            reload_data['robots'] = robots
            
            if len(robots) > 0:
                should_run = True


        is_running = await flink_service.is_job_running(saved_job_id)

        if is_running:
            logger.info(f"Job {saved_job_id} ({config_name}) is already RUNNING.")
            await kafka_service.create_topic(output_topic)
            await consumer_manager.start_consumer(output_topic)
            continue

        else:
            if not should_run:
                if config_type == 'SENSOR' and control_topic:
                    logger.info(f"Sending partial state for '{config_name}' to Kafka...")
                    await reload_sensor_state(control_topic, reload_data['sensors'], reload_data['robots'])

                if saved_job_id: 
                    logger.info(f"Cleaning up stale Job ID {saved_job_id} from database.")
                    database.update_job_status(config_name, None)
                continue
            

            count = 0
            if config_type == 'GEOFENCE':
                count = len(assignments)
            elif config_type == 'SENSOR':
                count = len(reload_data.get('sensors', [])) + len(reload_data.get('robots', []))
            elif config_type == 'COLLISION': 
                count = len(reload_data.get('robots', []))

            logger.warning(f"Job for '{config_name}' is DEAD but has {count} rules. Resurrecting...")
            
            try:
                control_topic = await flink_service.ensure_running(config_name)

                if config_type == 'GEOFENCE':
                    logger.info(f"Reloading Geofence rules for '{config_name}'...")
                    await reload_geofence_state(control_topic, reload_data['assignments'])
                    
                elif config_type == 'SENSOR':
                    logger.info(f"Reloading Sensor rules for '{config_name}'...")
                    await reload_sensor_state(control_topic, reload_data['sensors'], reload_data['robots'])
                
                elif config_type == 'COLLISION':
                    logger.info(f"Reloading Collision whitelist for '{config_name}'...")
                    await reload_collision_state(control_topic, reload_data['robots'])


                restored_count += 1
                logger.info(f"Configuration '{config_name}' successfully restored.")
                
            except Exception as e:
                logger.error(f"Failed to restore '{config_name}': {e}")

    logger.info(f"Recovery complete. Resurrected {restored_count} jobs.")


async def reload_geofence_state(topic: str, assignments: list):
    
    unique_zone_ids = set()
    robot_zone_map = {}

    for assignment in assignments:
        r_id = assignment['robot_id']
        z_id = assignment['zone_id']

        if z_id:
            unique_zone_ids.add(z_id)

        if r_id and z_id:
            if r_id not in robot_zone_map:
                robot_zone_map[r_id] = []
            robot_zone_map[r_id].append(z_id)

    for zone_id in unique_zone_ids:
        try:
            zone_data = database.get_zone(zone_id)
            if zone_data:
                await kafka_service.send_zone(ZoneCreate(**zone_data), topic)
        except Exception as e:
            logger.error(f"Failed to replay zone {zone_id}: {e}")

    for robot_id, zones_list in robot_zone_map.items():
        try:
            robot_data = database.get_robot(robot_id)
            if robot_data:
                await kafka_service.send_geofence_assignment(
                    robot_id=robot_id, 
                    zone_ids=zones_list, 
                    topic=topic
                )
        except Exception as e:
            logger.error(f"Failed to replay permissions for robot {robot_id}: {e}")


async def reload_sensor_state(topic: str, sensors: list, robots: list):
    for s in sensors:
        try:
            await kafka_service.send_sensor_assignment(
                sensor_id=s['sensor_id'],
                radius=s['radius'],
                threshold=s['humidity_threshold'],
                topic=topic
            )
        except Exception as e:
             logger.error(f"Failed to replay sensor {s['sensor_id']}: {e}")

    for r in robots:
        try:
            r_id = r['robot_id']
            
            if database.get_robot(r_id):
                await kafka_service.send_robot_assignment(
                    robot_id=r_id,
                    topic=topic
                )
        except Exception as e:
            logger.error(f"Failed to replay robot {r['robot_id']}: {e}")


async def reload_collision_state(topic: str, robots: list):
    for r in robots:
        try:
            r_id = r['robot_id']
            if database.get_robot(r_id):
                await kafka_service.send_robot_assignment(
                    robot_id=r_id,
                    topic=topic
                )
        except Exception as e:
            logger.error(f"Failed to replay robot {r.get('robot_id')} for collision: {e}")