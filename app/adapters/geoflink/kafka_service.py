import logging
import asyncio
from aiokafka import AIOKafkaProducer
from fastapi import HTTPException
from app.config import settings
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

logger = logging.getLogger("uvicorn.error")

# Kafka Communication and Infrastructure Service.
# This service manages the lifecycle of the AIOKafkaProducer and handles low-level Kafka operations.

class KafkaService:
    def __init__(self):
        self.producer = None
        self.broker_url = settings.KAFKA_BROKER

    async def start(self):
        logger.info(f"Connecting to Kafka Producer at {self.broker_url}...")
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.broker_url,
                    request_timeout_ms=30000 
                )
                
                await asyncio.wait_for(self.producer.start(), timeout=20.0)
                logger.info("Kafka Producer connected successfully.")
                return

            except Exception as e:
                logger.warning(f"Producer connection attempt {attempt}/{max_retries} failed: {repr(e)}")
                
                if self.producer:
                    try:
                        await self.producer.stop()
                    except:
                        pass
                    self.producer = None

                if attempt < max_retries:
                    await asyncio.sleep(2.0)
                else:
                    logger.error("CRITICAL: Failed to connect Kafka Producer. App will not be able to send commands.")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    async def _send_message(self, message: str, topic: str):
        if not self.producer:
            logger.error("Cannot send message: Producer is not connected.")
            raise HTTPException(503, detail="Kafka broker is unavailable (not connected)")

        try:
            await self.producer.send_and_wait(topic, value=message.encode('utf-8'))
            return {"status": "sent", "topic": topic, "payload": message}

        except Exception as e:
            logger.error(f"Failed to send to '{topic}': {e}")
            raise HTTPException(503, detail=f"Kafka communication error: {str(e)}")

    async def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        
        admin = AIOKafkaAdminClient(bootstrap_servers=self.broker_url)
        
        try:
            await admin.start()

            existing_topics = await admin.list_topics()
            
            if topic_name in existing_topics:
                logger.info(f"Topic '{topic_name}' already exists. Skipping creation.")
                return 

            logger.info(f"Topic '{topic_name}' not found in Kafka. Creating...")


            new_topic = NewTopic(
                name=topic_name, 
                num_partitions=num_partitions, 
                replication_factor=replication_factor
            )

            await admin.create_topics([new_topic])
            logger.info(f"Created new Kafka topic: {topic_name}")

        except TopicAlreadyExistsError:
            pass
            
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            
        finally:
            await admin.close()
    
    async def send_geofence_assignment(self, robot_id: str, zone_ids: list[str], topic: str):

        if not zone_ids:
            return 
            
        zones_str = ",".join(zone_ids)
        msg = f"ROBOT:ALLOW:{robot_id}:{zones_str}"
        
        return await self._send_message(msg, topic)

    async def send_geofence_unassignment(self, robot_id: str, zone_ids: list[str], topic: str):
        if not zone_ids:
            return

        zones_str = ",".join(zone_ids)
        msg = f"ROBOT:BLOCK:{robot_id}:{zones_str}"
        
        return await self._send_message(msg, topic)

    # Bans robot from all his  geofence assignments
    async def send_geofence_ban(self, robot_id: str, topic: str):
        msg = f"ROBOT:BLOCK:{robot_id}"
        return await self._send_message(msg, topic)


    async def send_zone(self, zone_data, topic: str):
        msg = f"ZONE:ADD:{zone_data.id}:{zone_data.geo}"
        return await self._send_message(msg, topic)
    
    async def send_zone_removal(self, zone_id: str, topic: str):
        msg = f"ZONE:DELETE:{zone_id}"
        return await self._send_message(msg, topic)

    # sensor proximity
    async def send_sensor_assignment(self, sensor_id: str, radius: float, threshold: float, topic: str):
        msg = f"SENSOR:ADD:{sensor_id}:{radius}:{threshold}"
        return await self._send_message(msg, topic)

    async def send_sensor_unassignment(self, sensor_id: str, topic: str):
        msg = f"SENSOR:REMOVE:{sensor_id}"
        return await self._send_message(msg, topic)

    # robot whitelist
    async def send_robot_assignment(self, robot_id: str, topic: str):
        msg = f"ROBOT:ALLOW:{robot_id}"
        return await self._send_message(msg, topic)

    async def send_robot_unassignment(self, robot_id: str, topic: str):
        msg = f"ROBOT:BLOCK:{robot_id}"
        return await self._send_message(msg, topic)

kafka_service = KafkaService()

def get_kafka_service() -> KafkaService:
    return kafka_service