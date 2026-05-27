from fastapi import APIRouter, HTTPException, Depends
import logging
from app.adapters.geoflink.schemas import RobotCreate
from app.adapters.geoflink import database
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service
from app.adapters.geoflink.kafka_service import KafkaService, get_kafka_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.post("/robots", tags=["Geoflink: Robots"])
def add_robot(robot: RobotCreate):
    existing_robot = database.get_robot(robot.id)
    
    if existing_robot:
        raise HTTPException(
            status_code=409, 
            detail=f"Robot with ID '{robot.id}' already exists. Use PUT method to update."
        )

    db_entry = database.RobotEntry(id=robot.id, status="ONLINE")
    database.upsert_robot(db_entry)

    logger.info(f"Robot registered: {robot.id}")
    return {"status": "registered", "id": robot.id}

@router.get("/robots", tags=["Geoflink: Robots"])
def list_robots():
    return database.get_all_robots()

@router.delete("/robots/{robot_id}", tags=["Geoflink: Robots"])
async def deregister_robot(
    robot_id: str,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):

    logger.info(f"Request to deregister robot: {robot_id}")

    active_assignments = database.find_all_jobs_for_robot(robot_id)
    
    configs_to_check = set()
    sent_topics = set() 

    for item in active_assignments:
        config_name = item['config_name']
        assignment_type = item['type']
        configs_to_check.add(config_name)
        
        state = database.get_config_state(config_name)
        if state:
            topic = state['control_topic']
            
            if topic and topic not in sent_topics:
                logger.info(f"Sending ban signal to topic: {topic} (Type: {assignment_type})")
                
                if assignment_type == 'GEOFENCE':
                    await kafka.send_geofence_ban(robot_id, topic)
                elif assignment_type == 'SENSOR':
                    await kafka.send_robot_unassignment(robot_id, topic)
                
                sent_topics.add(topic)

        if assignment_type == 'GEOFENCE':
            database.remove_geofence_assignment(robot_id, item['zone_id'], config_name)
        elif assignment_type == 'SENSOR':
            database.remove_robot_assignment(robot_id, config_name)

    database.delete_robot(robot_id)

    for config_name in configs_to_check:
        await flink.stop_if_empty(config_name)
    
    logger.info(f"Robot {robot_id} deleted. Removed from {len(active_assignments)} assignments.")

    return {
        "status": "deleted", 
        "robot_id": robot_id,
        "stopped_assignments": len(active_assignments)
    }

