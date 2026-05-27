import httpx
import logging
import json
from typing import Optional, List
from app.adapters.geoflink import database
from .schemas import JobConfigUnion
from pydantic import TypeAdapter
from fastapi import HTTPException
from app.adapters.geoflink.consumer_manager import consumer_manager
from app.config import settings
from .kafka_service import kafka_service
from asyncio import Lock

logger = logging.getLogger("uvicorn.info")

# Flink Job Lifecycle Orchestrator.
# This service manages the interaction between the FastAPI application and the Apache Flink cluster.
# It handles job submission (JAR execution), status monitoring, and graceful cancellation.


class FlinkService:
    def __init__(self, jar_name: str, base_url="http://localhost:8082"):
        self.jar_name = jar_name
        self.base_url = base_url
        self._lock = Lock()

    async def is_job_running(self, job_id: str) -> bool:
        if not job_id: 
            return False

        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                resp = await client.get(f"{self.base_url}/jobs/{job_id}")
                
                if resp.status_code == 404: 
                    return False
                
                if resp.status_code == 200:
                    state = resp.json().get('state')
                    return state == 'RUNNING'
                
                return False

            except Exception as e:
                logger.debug(f"Could not check status for job {job_id}")
                return False

    async def submit_job(self, jar_name_part: str, args_list: List[str], entry_class: Optional[str] = None) -> str:
        async with httpx.AsyncClient() as client:
            try:
                jars_resp = await client.get(f"{self.base_url}/jars")
                jars_resp.raise_for_status()
            except Exception as e:
                raise Exception(f"Error while downloading JAR list: {e}")

            files = jars_resp.json().get('files', [])
            jar_id = next((f['id'] for f in files if jar_name_part in f['name']), None)
            
            if not jar_id:
                raise Exception(f"Couldn't find .jar file with name: '{jar_name_part}'")

            payload = {
                "programArgsList": args_list  
            }
            
            if entry_class:
                payload["entryClass"] = entry_class

            logger.info(f"Submitting Flink job (Class: {entry_class})...")

            run_resp = await client.post(
                f"{self.base_url}/jars/{jar_id}/run",
                json=payload
            )
            
            if run_resp.status_code != 200:
                raise Exception(f"Flink error (code {run_resp.status_code}): {run_resp.text}")


            job_id = run_resp.json()['jobid']
            logger.info(f"Flink job submitted successfully. Job ID: {job_id}")

            return job_id

    async def stop_job(self, job_id: str):
        url = f"{self.base_url}/jobs/{job_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(url, params={"mode": "cancel"})

            if response.status_code == 404:
                logger.warning(f"Attempted to stop job {job_id}, but it was not found.")                
                return

            if response.status_code not in [200, 202]:
                raise Exception(f"Flink API Error {response.status_code}: {response.text}")            
            logger.info(f"Flink job {job_id} cancelled successfully.")

    async def ensure_running(self, config_name: str) -> str:
        async with self._lock:
            state = database.get_config_state(config_name)
            if not state:
                raise ValueError(f"Config '{config_name}' does not exist!")

            job_id = state['flink_job_id']
            topic_in = state['control_topic']
            topic_out = state['output_topic']
            

            if await self.is_job_running(job_id):
                await kafka_service.create_topic(topic_out)
                await consumer_manager.start_consumer(topic_out)
                return topic_in
            
            logger.info(f"Job for '{config_name}' is not running. Starting initialization...")
        
            try:
                await kafka_service.create_topic(topic_out)
                await kafka_service.create_topic(topic_in) 
                
            except Exception as e:
                logger.error(f"Failed to prepare Kafka topics for {config_name}: {e}")
                raise HTTPException(500, "Kafka Infrastructure Error")


            try:
                json_str = state['config_json']
                adapter = TypeAdapter(JobConfigUnion)
                config_obj = adapter.validate_json(json_str)
                args = config_obj.to_flink_args(topic_in, topic_out)

                target_class = config_obj.get_entry_class()
                
            except Exception as e:
                logger.error(f"Config corruption for {config_name}: {e}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Configuration '{config_name}' is corrupted or incompatible. Check logs."
                )

            try:
                new_job_id = await self.submit_job(
                    jar_name_part=self.jar_name, 
                    args_list=args,
                    entry_class=target_class
                )
            except Exception as e:
                logger.error(f"Flink submission failed: {e}")            
                raise HTTPException(
                    status_code=503, 
                    detail=f"Failed to start Flink job. Cluster might be busy or down. Error: {str(e)}"
                )

            try:
                database.update_job_status(config_name, new_job_id)
            except Exception as e:
                logger.critical(f"DB Update failed after Flink submit! Rolling back job {new_job_id}...")            
                try:
                    await self.stop_job(new_job_id) 
                except Exception as rollback_ex:
                    logger.critical(f"FATAL: Failed to stop zombie job {new_job_id}: {rollback_ex}")
                
                raise HTTPException(
                    status_code=500, 
                    detail="System consistency error. Job started but DB update failed."
                )
            
            await consumer_manager.start_consumer(topic_out)
            return topic_in
                
    async def stop_if_empty(self, config_name: str):
        state = database.get_config_state(config_name)
        if not state:
            logger.warning(f"Config '{config_name}' not found during stop check.")
            return

        config_def = json.loads(state['config_json'])
        config_type = config_def.get('type', 'GEOFENCE') 

        should_stop = False
        
        if config_type == 'SENSOR':
            sensors = database.get_sensor_assignments(config_name)
            robots = database.get_robot_assignments(config_name)
            
            if len(sensors) == 0 or len(robots) == 0:
                logger.info(f"Checking stop condition for '{config_name}' (SENSOR): S={len(sensors)}, R={len(robots)} -> STOP REQUIRED")
                should_stop = True
            else:
                logger.debug(f"Checking stop condition for '{config_name}' (SENSOR): Pair exists. KEEP RUNNING.")
        
        elif config_type == 'COLLISION':
            robots = database.get_robot_assignments(config_name)
            
            if len(robots) == 0:
                logger.info(f"Checking stop condition for '{config_name}' (COLLISION): Robot count=0 -> STOP REQUIRED")
                should_stop = True
            else:
                logger.debug(f"Checking stop condition for '{config_name}' (COLLISION): {len(robots)} robots active. KEEP RUNNING.")
        else: 
            count = database.count_active_assignments(config_name)
            if count == 0:
                logger.info(f"Checking stop condition for '{config_name}' (GEOFENCE): Count={count} -> STOP REQUIRED")
                should_stop = True
            else:
                logger.debug(f"Checking stop condition for '{config_name}' (GEOFENCE): Active rules present. KEEP RUNNING.")

        if should_stop:
            if state['flink_job_id']:
                logger.info(f"Stopping Job {state['flink_job_id']} for config '{config_name}'...")
                try:
                    await self.stop_job(state['flink_job_id'])
                except Exception as e:
                    logger.warning(f"Could not stop job: {e}")

                database.update_job_status(config_name, None)
            
            topic_out = state['output_topic']
            if topic_out:
                await consumer_manager.stop_consumer(topic_out)
                
            logger.info(f"Configuration '{config_name}' successfully deactivated.")
        else:
            pass

def get_flink_service():
    return FlinkService(
        base_url=settings.FLINK_URL,
        jar_name=settings.FLINK_JAR_NAME
    )