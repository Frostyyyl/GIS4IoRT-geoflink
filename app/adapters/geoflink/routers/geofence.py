from fastapi import APIRouter, HTTPException, Depends
import json
import logging
from app.adapters.geoflink.schemas import ZoneCreate, RobotCreate, GeofenceRequest
from app.adapters.geoflink import database
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service
from app.adapters.geoflink.kafka_service import KafkaService, get_kafka_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.get("/geofence/{config_name}", tags=["Geoflink: Geofence"])
def list_geofence_rules(config_name: str):

    config_state = database.get_config_state(config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{config_name}' not found.")

    config_def = json.loads(config_state['config_json'])
    if config_def.get('type') != 'GEOFENCE':
         raise HTTPException(400, f"Configuration '{config_name}' is not GEOFENCE type.")

    rules = database.get_geofence_assignments(config_name)
    
    return {
        "config_name": config_name,
        "count": len(rules),
        "rules": rules
    }


@router.post("/geofence", tags=["Geoflink: Geofence"])  
async def add_geofence_rule(
    data: GeofenceRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to add rule for config '{data.config_name}' (R: {data.robot_id}, Z: {data.zone_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")


    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'GEOFENCE':
        raise HTTPException(400, f"Configuration '{data.config_name}' was not created with geofencing in mind! This configuration type is: {config_def.get('type')}.")

    robot_obj = None
    if data.robot_id:
        robot_dict = database.get_robot(data.robot_id)
        if not robot_dict:
            raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist.")
        
        robot_obj = RobotCreate(**robot_dict)

    zone_obj = None
    if data.zone_id:
        zone_dict = database.get_zone(data.zone_id)
        if not zone_dict:
            raise HTTPException(404, detail=f"Zone '{data.zone_id}' does not exist.")
        
        zone_obj = ZoneCreate(**zone_dict)

    existing_assignment = database.get_geofence_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id, 
        zone_id=data.zone_id
    )
    
    if existing_assignment:
        raise HTTPException(
            status_code=409,
            detail=f"Rule already exists")


    topic = await flink.ensure_running(data.config_name)

    logger.info(f"Sending configuration payload to Kafka topic: {topic}")

    if zone_obj:
        await kafka.send_zone(zone_obj, topic)

    
    await kafka.send_geofence_assignment(
        robot_id=data.robot_id, 
        zone_ids=[data.zone_id], 
        topic=topic
    )

    database.add_geofence_assignment(data.robot_id, data.zone_id, data.config_name)
    
    logger.info(f"Rule assigned successfully for '{data.config_name}'")

    return {"status": "assigned"}


@router.delete("/geofence", tags=["Geoflink: Geofence"])
async def remove_geofence_rule(
    data: GeofenceRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to remove rule from '{data.config_name}' (R: {data.robot_id}, Z: {data.zone_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")

    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'GEOFENCE':
        raise HTTPException(400, f"Configuration '{data.config_name}' is not GEOFENCE type.")

    assignment = database.get_geofence_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id, 
        zone_id=data.zone_id
    )

    if not assignment:
        detail_msg = f"Assignment for config '{data.config_name}'"
        if data.robot_id: detail_msg += f" with robot '{data.robot_id}'"
        if data.zone_id: detail_msg += f" and zone '{data.zone_id}'"
        detail_msg += " not found."
        
        raise HTTPException(status_code=404, detail=detail_msg)


    topic = config_state['control_topic']

    if topic:

        logger.info(f"Sending removal signals to Kafka topic: {topic}")

        
        await kafka.send_geofence_unassignment(
        robot_id=data.robot_id, 
        zone_ids=[data.zone_id], 
        topic=topic
        )

    database.remove_geofence_assignment(data.robot_id, data.zone_id, data.config_name)
    
    await flink.stop_if_empty(data.config_name)
    
    logger.info(f"Rule removed from '{data.config_name}'")

    return {"status": "removed"}
