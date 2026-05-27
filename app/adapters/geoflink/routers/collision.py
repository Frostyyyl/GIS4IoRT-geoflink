from fastapi import APIRouter, HTTPException, Depends
import json
import logging
from app.adapters.geoflink import database
from app.adapters.geoflink.schemas import RobotRequest
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service
from app.adapters.geoflink.kafka_service import KafkaService, get_kafka_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

def validate_collision_config(config_name: str):
    config_state = database.get_config_state(config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{config_name}' not found.")

    try:
        config_def = json.loads(config_state['config_json'])
    except json.JSONDecodeError:
        raise HTTPException(500, f"Invalid JSON in configuration '{config_name}'.")

    if config_def.get('type') != 'COLLISION':
        raise HTTPException(400, f"Configuration '{config_name}' is not COLLISION type. (Current type: {config_def.get('type')})")
    
    return config_state, config_def


@router.get("/collision/{config_name}", tags=["Geoflink: Collision Detection"])
def list_collision_robots(config_name: str):

    _, _ = validate_collision_config(config_name)

    robots = database.get_robot_assignments(config_name)
    
    return {
        "config_name": config_name,
        "count_robots": len(robots),
        "robots": robots
    }


@router.post("/collision", tags=["Geoflink: Collision Detection"])  
async def add_collision_robot(
    data: RobotRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to add robot to collision config '{data.config_name}' (Robot: {data.robot_id})")

    config_state, _ = validate_collision_config(data.config_name)

    robot_dict = database.get_robot(data.robot_id)
    if not robot_dict:
        raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist in system inventory.")

    existing_assignment = database.get_robot_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id
    )
    
    if existing_assignment:
        raise HTTPException(status_code=409, detail=f"Robot '{data.robot_id}' is already monitored in '{data.config_name}'")

    logger.info(f"Ensuring Flink job is running for '{data.config_name}'...")
    topic = await flink.ensure_running(data.config_name)
    
    logger.info(f"Sending ALLOW signal for {data.robot_id} to topic: {topic}")
    
    await kafka.send_robot_assignment(
        robot_id=data.robot_id, 
        topic=topic
    )

    database.add_robot_assignment(data.robot_id, data.config_name)
    
    logger.info(f"Robot '{data.robot_id}' added to collision config '{data.config_name}'")

    return {"status": "assigned", "robot_id": data.robot_id, "config": data.config_name}


@router.delete("/collision", tags=["Geoflink: Collision Detection"])
async def remove_collision_robot(
    data: RobotRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):

    logger.info(f"Request to remove robot from collision config '{data.config_name}' (Robot: {data.robot_id})")

    config_state, _ = validate_collision_config(data.config_name)

    assignment = database.get_robot_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id
    )

    if not assignment:
        raise HTTPException(status_code=404, detail=f"Robot '{data.robot_id}' is not assigned to config '{data.config_name}'.")

    topic = config_state['control_topic']
    
    if topic:
        logger.info(f"Sending BLOCK signal for {data.robot_id} to topic: {topic}")
        await kafka.send_robot_unassignment(
            robot_id=data.robot_id, 
            topic=topic
        )

    database.remove_robot_assignment(data.robot_id, data.config_name) 
    
    await flink.stop_if_empty(data.config_name)
    
    logger.info(f"Robot '{data.robot_id}' removed from collision config '{data.config_name}'")

    return {"status": "removed", "robot_id": data.robot_id}