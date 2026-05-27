import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.config import settings
from .websocket_manager import websocket_manager

logger = logging.getLogger("uvicorn.info")

logging.getLogger("aiokafka").setLevel(logging.CRITICAL)

# Kafka Consumer Manager for WebSocket Broadcasting.
# This module manages background Kafka consumers using aiokafka.

class ConsumerManager:
    def __init__(self):
        self.active_tasks = {}

    async def start_consumer(self, topic_name: str):
        if topic_name in self.active_tasks:
            self.active_tasks[topic_name]["count"] += 1
            logger.info(f"Topic '{topic_name}': Client joined (Total: {self.active_tasks[topic_name]['count']})")            
            return

        logger.info(f"Starting background consumer for topic: {topic_name}")
        
        
        consumer = None
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                consumer = AIOKafkaConsumer(
                    topic_name,
                    bootstrap_servers=settings.KAFKA_BROKER,
                    group_id=f"fastapi_shared_consumer_{topic_name}",
                    auto_offset_reset='latest',
                    
                    request_timeout_ms=30000,   
                    session_timeout_ms=60000,   
                    heartbeat_interval_ms=20000 
                )
                await asyncio.wait_for(consumer.start(), timeout=10.0)
                logger.info(f"Consumer started for {topic_name}")
                break 
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{max_retries} failed to start consumer for {topic_name}: {repr(e)}")
                
                if consumer:
                    try:
                        await asyncio.wait_for(consumer.stop(), timeout=5.0)
                    except:
                        pass
                
                if attempt == max_retries:
                    logger.error(f"Failed to start consumer for '{topic_name}' after {max_retries} attempts.")                    
                    return
                
                await asyncio.sleep(2.0)

        loop = asyncio.get_event_loop()
        task = loop.create_task(self._consume_loop(topic_name, consumer))
        
        self.active_tasks[topic_name] = {
            "task": task,
            "consumer": consumer,
            "count": 1
        }

    async def stop_consumer(self, topic_name: str):
        if topic_name not in self.active_tasks:
            return
        
        self.active_tasks[topic_name]["count"] -= 1
        current_count = self.active_tasks[topic_name]["count"]

        if current_count > 0:
             logger.info(f"Topic '{topic_name}': Client left (Remaining: {current_count})")
        else:
            logger.info(f"Ref count is 0. Shutting down consumer for '{topic_name}'...")
            
            entry = self.active_tasks.pop(topic_name) 
            task = entry["task"]
            consumer = entry["consumer"]

            task.cancel()

            try:
                await asyncio.wait_for(consumer.stop(), timeout=1.0)
            except Exception:
                pass

            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Consumer for {topic_name} successfully stopped.")

    async def _consume_loop(self, topic_name: str, consumer: AIOKafkaConsumer):
        try:
            async for msg in consumer:
                payload = msg.value.decode('utf-8')
                await websocket_manager.broadcast_to_topic(topic_name, payload)

        except asyncio.CancelledError:
            pass
            
        except Exception as e:
            logger.error(f"Error in consume loop for {topic_name}: {e}")
            
        finally:
            try:
                await asyncio.wait_for(consumer.stop(), timeout=0.5)
            except Exception:
                pass

    async def stop_all(self):
        logger.info("Stopping ALL consumers...")
        
        topics = list(self.active_tasks.keys())
        
        for topic in topics:
            entry = self.active_tasks.pop(topic)
            task = entry.get("task")
            consumer = entry.get("consumer")

            if task:
                task.cancel()

            if consumer:
                try:
                    await asyncio.wait_for(consumer.stop(), timeout=1.0)
                except (Exception, asyncio.TimeoutError) as e:
                    logger.warning(f"Forced consumer stop for '{topic}' due to error: {repr(e)}")

            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error awaiting task for {topic}: {e}")
        
        logger.info("All consumers stopped.")

consumer_manager = ConsumerManager()