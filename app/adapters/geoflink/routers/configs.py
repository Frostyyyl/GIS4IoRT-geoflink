from fastapi import APIRouter, HTTPException, Depends
import logging
from app.adapters.geoflink.schemas import JobConfigUnion
from app.adapters.geoflink import database
from app.adapters.geoflink.flink_service import FlinkService, get_flink_service

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.post("/config", tags=["Geoflink: Config"])
def register_job_config(config: JobConfigUnion):
    existing_config = database.get_config_state(config.name)
    
    if existing_config:
        raise HTTPException(
            status_code=409,  
            detail=f"Configuration with name '{config.name}' already exists. Delete it first if you want to recreate it."
        )
    
    database.create_config(config.name, config.model_dump())

    logger.info(f"Config created: {config.name} (Type: {config.type})")

    return {
        "status": "created", 
        "type": config.type, 
        "control_topic": f"control_{config.name}", 
        "output_topic": f"output_{config.name}"    
    }

@router.get("/config", tags=["Geoflink: Config"])
def list_configs():
    return database.get_configs_formatted()

@router.delete("/config/{config_name}", tags=["Geoflink: Config"])
async def delete_job_config(
    config_name: str,
    flink: FlinkService = Depends(get_flink_service)
):
    state = database.get_config_state(config_name)
    if not state:
        raise HTTPException(status_code=404, detail="Config not found")

    logger.info(f"Request to delete config: {config_name}")

    job_id = state['flink_job_id']
    stopped_job_id = None

    if job_id:

        logger.info(f"Stopping Flink job {job_id} associated with '{config_name}'...")
        try:
            await flink.stop_job(job_id)
            stopped_job_id = job_id
            logger.info(f"Job {job_id} stopped successfully.")
        except Exception as e:
            logger.warning(f"Could not stop Flink job (might be already stopped or unreachable): {e}")

    database.delete_config(config_name)

    logger.info(f"Config '{config_name}' removed from database.")

    return {
        "status": "deleted", 
        "name": config_name, 
        "stopped_job": stopped_job_id
    }