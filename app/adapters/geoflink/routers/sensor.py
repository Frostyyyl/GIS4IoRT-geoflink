from fastapi import APIRouter, HTTPException, Depends
import json
import logging
from app.adapters.geoflink import database
from app.adapters.geoflink.schemas import SensorRequest, RobotRequest
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service
from app.adapters.geoflink.kafka_service import KafkaService, get_kafka_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.get("/sensor/{config_name}", tags=["Geoflink: Sensor Proximity"])
def list_sensor_rules(config_name: str):
    config_state = database.get_config_state(config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{config_name}' not found.")

    config_def = json.loads(config_state['config_json'])
    if config_def.get('type') != 'SENSOR':
         raise HTTPException(400, f"Configuration '{config_name}' is not SENSOR type.")

    sensors = database.get_sensor_assignments(config_name)
    robots = database.get_robot_assignments(config_name)   
    return {
        "config_name": config_name,
        "count_sensors": len(sensors),
        "sensors": sensors,
        "count_robots": len(robots),
        "robots": robots
    }


@router.post("/sensor", tags=["Geoflink: Sensor Proximity"])  
async def add_sensor_rule(
    data: SensorRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to add rule for config '{data.config_name}' (S: {data.sensor_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")


    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'SENSOR':
        raise HTTPException(400, f"Configuration '{data.config_name}' was not created with sensor proximity in mind! This configuration type is: {config_def.get('type')}.")


    existing_assignment = database.get_sensor_assignment(
        config_name=data.config_name,
        sensor_id=data.sensor_id
    )
    
    if existing_assignment:
        raise HTTPException(
            status_code=409,
            detail=f"Rule already exists")


    existing_robots = database.get_robot_assignments(data.config_name)
    
    if len(existing_robots) > 0:
        logger.info(f"Config '{data.config_name}' has robots. Ensuring Flink is running.")
        topic = await flink.ensure_running(data.config_name)
    else:
        logger.info(f"Config '{data.config_name}' has no robots yet. Skipping Flink startup.")
        topic = config_state['control_topic']

    logger.info(f"Sending configuration payload to Kafka topic: {topic}")
    
    await kafka.send_sensor_assignment(
        sensor_id = data.sensor_id, 
        radius = data.radius,
        threshold = data.humidity_threshold,
        topic=topic
    )

    database.add_sensor_assignment(data.sensor_id, data.radius, data.humidity_threshold, data.config_name)
    
    logger.info(f"Rule assigned successfully for '{data.config_name}'")

    return {"status": "assigned"}



@router.post("/sensor/robot", tags=["Geoflink: Sensor Proximity"])  
async def add_sensor_robot_rule(
    data: RobotRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to add rule for config '{data.config_name}' (R: {data.robot_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")


    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'SENSOR':
        raise HTTPException(400, f"Configuration '{data.config_name}' was not created with sensor proximity in mind! This configuration type is: {config_def.get('type')}.")


    existing_assignment = database.get_robot_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id
    )
    
    if existing_assignment:
        raise HTTPException(
            status_code=409,
            detail=f"Rule already exists")


    robot_dict = database.get_robot(data.robot_id)
    if not robot_dict:
        raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist.")
        

    existing_sensors = database.get_sensor_assignments(data.config_name)
    
    if len(existing_sensors) > 0:
        logger.info(f"Config '{data.config_name}' has sensors. Ensuring Flink is running.")
        topic = await flink.ensure_running(data.config_name)
    else:
        logger.info(f"Config '{data.config_name}' has no sensors yet. Skipping Flink startup.")
        topic = config_state['control_topic']
    logger.info(f"Sending configuration payload to Kafka topic: {topic}")
    
    await kafka.send_robot_assignment(
        robot_id=data.robot_id, 
        topic=topic
    )

    database.add_robot_assignment(data.robot_id, data.config_name)
    
    logger.info(f"Rule assigned successfully for '{data.config_name}'")

    return {"status": "assigned"}


@router.delete("/sensor", tags=["Geoflink: Sensor Proximity"])
async def remove_sensor_rule(
    data: SensorRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to remove rule from '{data.config_name}' (S: {data.sensor_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")

    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'SENSOR':
        raise HTTPException(400, f"Configuration '{data.config_name}' is not SENSOR type.")

    assignment = database.get_sensor_assignment(
        config_name=data.config_name,
        sensor_id=data.sensor_id
    )

    if not assignment:
        detail_msg = f"Assignment for config '{data.config_name}'"
        if data.sensor_id: detail_msg += f" with sensor '{data.sensor_id}'"
        detail_msg += " not found."
        
        raise HTTPException(status_code=404, detail=detail_msg)


    topic = config_state['control_topic']

    if topic:

        logger.info(f"Sending removal signals to Kafka topic: {topic}")

        await kafka.send_sensor_unassignment(
        sensor_id=data.sensor_id, 
        topic=topic
        )

    database.remove_sensor_assignment(data.sensor_id, data.config_name) 
    
    await flink.stop_if_empty(data.config_name)
    
    logger.info(f"Rule removed from '{data.config_name}'")

    return {"status": "removed"}


@router.delete("/sensor/robot", tags=["Geoflink: Sensor Proximity"])
async def remove_sensor_robot_rule(
    data: RobotRequest,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to remove rule from '{data.config_name}' (R: {data.robot_id})")

    config_state = database.get_config_state(data.config_name)
    if not config_state:
        raise HTTPException(404, f"Configuration '{data.config_name}' not found.")

    config_def = json.loads(config_state['config_json'])
    
    if config_def.get('type') != 'SENSOR':
        raise HTTPException(400, f"Configuration '{data.config_name}' is not SENSOR type.")

    assignment = database.get_robot_assignment(
        config_name=data.config_name,
        robot_id=data.robot_id
    )

    if not assignment:
        detail_msg = f"Assignment for config '{data.config_name}'"
        if data.robot_id: detail_msg += f" with robot '{data.robot_id}'"
        detail_msg += " not found."
        
        raise HTTPException(status_code=404, detail=detail_msg)


    topic = config_state['control_topic']

    if topic:

        logger.info(f"Sending removal signals to Kafka topic: {topic}")

        await kafka.send_robot_unassignment(
        robot_id=data.robot_id, 
        topic=topic
        )

    database.remove_robot_assignment(data.robot_id, data.config_name) 
    
    await flink.stop_if_empty(data.config_name)
    
    logger.info(f"Rule removed from '{data.config_name}'")

    return {"status": "removed"}

