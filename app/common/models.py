from pydantic import BaseModel
from typing import Optional

# Common models for all adapters

class GPSPoint(BaseModel):
    """Pojedynczy punkt GPS"""
    timestamp: float
    latitude: float
    longitude: float

class RobotBase(BaseModel):
    """Bazowy model robota"""
    id: str
    status: Optional[str] = "ACTIVE"

class ZoneBase(BaseModel):
    """Bazowy model strefy"""
    id: str
    geo: str  # WKB HEX format
    name: Optional[str] = None