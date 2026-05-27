from fastapi import APIRouter

from .websockets import router as ws_router
from .robots import router as robots_router
from .zones import router as zones_router
from .configs import router as configs_router
from .geofence import router as geofence_router
from .sensor import router as sensor_router
from .collision import router as collision_router

router = APIRouter()

router.include_router(robots_router)
router.include_router(zones_router)
router.include_router(configs_router)
router.include_router(geofence_router)
router.include_router(collision_router)
router.include_router(sensor_router)
router.include_router(ws_router)


