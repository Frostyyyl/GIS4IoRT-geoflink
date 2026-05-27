import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from . import database
from .kafka_service import kafka_service
from .consumer_manager import consumer_manager
from .recovery_service import restore_application_state

logger = logging.getLogger("uvicorn.info")

# Manages the global startup and shutdown sequences of the Geoflink module.

@asynccontextmanager
async def geoflink_lifespan(app: FastAPI):

    module_name = "[Geoflink]"
    logger.info(f"{module_name} Starting up...")
    try:
        database.init_db()
        logger.info(f"{module_name} Database initialized.")
    except Exception as e:
        logger.critical(f"{module_name} FATAL: Database initialization failed: {e}")
        raise e
    
    await kafka_service.start()
    try:
        await restore_application_state()
    except Exception as e:
        logger.error(f"{module_name} State recovery failed: {e}")
    logger.info(f"{module_name} Module is READY.")

    yield  
    
    logger.info(f"{module_name} Shutting down...")
    await consumer_manager.stop_all()
    await kafka_service.stop()
    logger.info(f"{module_name} Shutdown complete.")