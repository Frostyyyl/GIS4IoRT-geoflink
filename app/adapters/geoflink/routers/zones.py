from fastapi import APIRouter, HTTPException, Depends
import logging
from app.adapters.geoflink.schemas import ZoneCreate
from app.adapters.geoflink import database
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service
from app.adapters.geoflink.kafka_service import KafkaService, get_kafka_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.post("/zones", tags=["Geoflink: Zones"])
def add_zone(zone: ZoneCreate):

    existing_zone = database.get_zone(zone.id)
    
    if existing_zone:
        raise HTTPException(
            status_code=409, 
            detail=f"Zone with ID '{zone.id}' already exists."
        )

    
    db_entry = database.ZoneEntry(id=zone.id, geo=zone.geo)
    database.upsert_zone(db_entry)

    logger.info(f"Zone created: {zone.id}")
    
    return {"status": "zone_created", "id": zone.id}

# TODO consider adding PUT requests for updating the data

@router.get("/zones", tags=["Geoflink: Zones"])
def list_zones():
    return database.get_all_zones()

@router.delete("/zones/{zone_id}", tags=["Geoflink: Zones"])
async def deregister_zone(
    zone_id: str,
    flink: FlinkService = Depends(get_flink_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    logger.info(f"Request to delete zone: {zone_id}")

    active_assignments = database.find_all_jobs_for_zone(zone_id)
    configs_to_check = set()

    for item in active_assignments:
        config_name = item['config_name']
        configs_to_check.add(config_name)
        
        state = database.get_config_state(config_name)
        if state:
            topic = state['control_topic']

            logger.info(f"Sending zone removal signal to Kafka topic: {topic}")

            await kafka.send_zone_removal(zone_id, topic)
            database.remove_geofence_assignment(item['robot_id'], zone_id, config_name)

    database.delete_zone(zone_id)

    for config_name in configs_to_check:
        await flink.stop_if_empty(config_name)
    
    logger.info(f"Zone {zone_id} deleted. Cleaned {len(active_assignments)} assignments.")

    return {
        "status": "deleted", 
        "zone_id": zone_id,
        "stopped_assignments": len(active_assignments)
    }