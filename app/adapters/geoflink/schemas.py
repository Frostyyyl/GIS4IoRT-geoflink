from pydantic import BaseModel, Field, model_validator, validator
from typing import List, Literal, Union, Optional, Annotated
import re
import json
import base64

# Data Schemas and Configuration Models.
# This module defines the core data structures using Pydantic for the GeoFlink API.

class BaseJobConfig(BaseModel):
    name: str
    parallelism: int = Field(default=1, ge=1, description="Flink parallelism must be >= 1")    
    bootStrapServers: str = "broker:29092"
    localWebUi: bool = Field(default=False, description="Start local Web UI (for debug only)")

    def to_flink_args(self, control_topic: str, output_topic: str) -> List[str]:
        raise NotImplementedError()
    
    def get_entry_class(self) -> str:
        raise NotImplementedError("Each config type must define its own Entry Class!")

class GeofenceConfig(BaseJobConfig):
    type: Literal["GEOFENCE"]
    cellLengthMeters: float = Field(default=0, ge=0)
    uniformGridSize: int = Field(default=100, ge=0)
    gridMinX: float 
    gridMinY: float 
    gridMaxX: float 
    gridMaxY: float 
    inputTopicName: str = Field(
        default="multi_gps_fix", 
        description="Kafka topic for robot telemetry"
    )
    range: float = Field(default=0.00000000001, gt=0)

    @model_validator(mode='after')
    def check_coordinates(self):
        if self.gridMinX >= self.gridMaxX:
            raise ValueError(f"gridMinX ({self.gridMinX}) must be smaller than gridMaxX ({self.gridMaxX})")
        if self.gridMinY >= self.gridMaxY:
            raise ValueError(f"gridMinY ({self.gridMinY}) must be smaller than gridMaxY ({self.gridMaxY})")
        return self


    def to_flink_args(self, control_topic: str, output_topic: str) -> list[str]:
       
        config_dict = {
            "parallelism": self.parallelism,
            "bootStrapServers": self.bootStrapServers,
            "localWebUi": self.localWebUi,  
            "configName": self.name,

            "cellLengthMeters": self.cellLengthMeters,
            "uniformGridSize": self.uniformGridSize,
            "gridMinX": self.gridMinX,
            "gridMinY": self.gridMinY,
            "gridMaxX": self.gridMaxX,
            "gridMaxY": self.gridMaxY,
            

            "inputTopicName": self.inputTopicName,
            "outputTopicName": output_topic,
            "controlTopicName": control_topic,
            
            "radius": self.range  
        }

        json_str = json.dumps(config_dict)

        base64_config = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        return [
            "--configBase64", base64_config
        ]

    def get_entry_class(self) -> str:
        return "GIS4IoRT.jobs.GeofencingStreamingJob"
    

class SensorProximityConfig(BaseJobConfig):
    type: Literal["SENSOR"]
    
    cellLengthMeters: float = Field(default=0, ge=0)
    uniformGridSize: int = Field(default=100, ge=0)
    gridMinX: float
    gridMinY: float
    gridMaxX: float 
    gridMaxY: float 

    inputTopicName: str = Field(
        default="multi_gps_fix", 
        description="Kafka topic for robot telemetry"
    )
    sensorTopicName: str = Field(
        default="sensor_proximity", 
        description="Kafka topic for sensor readings"
    )
    
    @model_validator(mode='after')
    def check_coordinates(self):
        if self.gridMinX >= self.gridMaxX:
            raise ValueError(f"gridMinX ({self.gridMinX}) must be smaller than gridMaxX ({self.gridMaxX})")
        if self.gridMinY >= self.gridMaxY:
            raise ValueError(f"gridMinY ({self.gridMinY}) must be smaller than gridMaxY ({self.gridMaxY})")
        return self

    def to_flink_args(self, control_topic: str, output_topic: str) -> List[str]:

        config_dict = {
            "parallelism": self.parallelism,
            "bootStrapServers": self.bootStrapServers,
            "localWebUi": self.localWebUi,
            "configName": self.name, 

            "cellLengthMeters": self.cellLengthMeters,
            "uniformGridSize": self.uniformGridSize,
            "gridMinX": self.gridMinX,
            "gridMinY": self.gridMinY,
            "gridMaxX": self.gridMaxX,
            "gridMaxY": self.gridMaxY,
            
            "inputTopicName": self.inputTopicName,
            "sensorTopicName": self.sensorTopicName, 
            "outputTopicName": output_topic,
            "controlTopicName": control_topic
        }

        json_str = json.dumps(config_dict)
        base64_config = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        return [
            "--configBase64", base64_config
        ]

    def get_entry_class(self) -> str:
        return "GIS4IoRT.jobs.SensorProximityStreamingJob"


class CollisionDetectionConfig(BaseJobConfig):
    type: Literal["COLLISION"]
    
    cellLengthMeters: float = Field(default=0, ge=0)
    uniformGridSize: int = Field(default=100, ge=0)
    gridMinX: float
    gridMinY: float
    gridMaxX: float
    gridMaxY: float

    inputTopicName: str = Field(
        default="multi_gps_fix", 
        description="Kafka topic for robot telemetry"
    )

    collisionThreshold: float = Field(
        default=1.5, 
        gt=0, 
        description="Distance in meters to trigger collision alert"
    )
    robotStateTtlMillis: int = Field(
        default=5000, 
        ge=1, 
        description="Time to live for robot state (milliseconds)"
    )
    robotAlertCooldownMillis: int = Field(
        default=5000, 
        ge=0, 
        description="Minimum time between alerts for the same pair (milliseconds)"
    )
    
    @model_validator(mode='after')
    def check_coordinates(self):
        if self.gridMinX >= self.gridMaxX:
            raise ValueError(f"gridMinX ({self.gridMinX}) must be smaller than gridMaxX ({self.gridMaxX})")
        if self.gridMinY >= self.gridMaxY:
            raise ValueError(f"gridMinY ({self.gridMinY}) must be smaller than gridMaxY ({self.gridMaxY})")
        return self

    def to_flink_args(self, control_topic: str, output_topic: str) -> List[str]:

        config_dict = {
            "parallelism": self.parallelism,
            "bootStrapServers": self.bootStrapServers,
            "localWebUi": self.localWebUi,
            "configName": self.name, 

            "cellLengthMeters": self.cellLengthMeters,
            "uniformGridSize": self.uniformGridSize,
            "gridMinX": self.gridMinX,
            "gridMinY": self.gridMinY,
            "gridMaxX": self.gridMaxX,
            "gridMaxY": self.gridMaxY,
            
            "inputTopicName": self.inputTopicName,
            "outputTopicName": output_topic, 
            "controlTopicName": control_topic, 

            "collisionThreshold": self.collisionThreshold,
            "robotStateTtlMillis": self.robotStateTtlMillis,
            "robotAlertCooldownMillis": self.robotAlertCooldownMillis
        }

        json_str = json.dumps(config_dict)
        base64_config = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        return [
            "--configBase64", base64_config
        ]

    def get_entry_class(self) -> str:
        return "GIS4IoRT.jobs.CollisionDetectionStreamingJob"


JobConfigUnion = Annotated[
    Union[GeofenceConfig, SensorProximityConfig, CollisionDetectionConfig],
    Field(discriminator="type")
]

# geofence
class GeofenceRequest(BaseModel):
    robot_id: str = Field(..., min_length=1)
    zone_id: str = Field(..., min_length=1)
    config_name: str = Field(..., min_length=1)


# sensor proximity
class RobotRequest(BaseModel):
    config_name: str = Field(..., min_length=1)
    robot_id: str = Field(..., min_length=1)

class SensorRequest(BaseModel):
    sensor_id: str = Field(..., min_length=1)
    radius: float = Field(gt=0)
    humidity_threshold: float = Field(ge=0, le=100)
    config_name: str = Field(..., min_length=1)


   

    
# for SQL
class RobotEntry(BaseModel):
    id: str
    status: str

class ZoneEntry(BaseModel):
    id: str
    geo: str #WKB hex


# for API
class ZoneCreate(BaseModel):
    id: str = Field(
        ..., 
        min_length=1, 
        strip_whitespace=True,
        description="Unique zone ID"
    )
    
    geo: str = Field(
        ..., 
        min_length=10,
        description="Geometry in WKB HEX String Format"
    )

    @validator('geo')
    def validate_wkb_hex(cls, v):
        if not re.fullmatch(r'^[0-9A-Fa-f]+$', v):
            raise ValueError('Geometry has to be a correct HEX String')
        
        if len(v) % 2 != 0:
            raise ValueError('Wrong length of the WKB HEX String (has to be even)')
            
        return v.upper()

class RobotCreate(BaseModel):
    id: str = Field(
        ..., 
        min_length=1, 
        max_length=100,           
        strip_whitespace=True,
        description="Unique robot ID"
    )