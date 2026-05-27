from fastapi import WebSocket
from typing import Dict, List, Set
import logging
import json

logger = logging.getLogger("uvicorn.info")

# WebSocket Connection and Pub/Sub Manager.
# This module handles real-time communication between the server and multiple clients.

class ConnectionManager:
    def __init__(self):
        self.subscriptions: Dict[str, List[WebSocket]] = {}
        self.socket_subscriptions: Dict[WebSocket, Set[str]] = {}

    def _get_client_id(self, websocket: WebSocket) -> str:
        if websocket.client:
            return f"{websocket.client.host}:{websocket.client.port}"
        return "unknown"

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.socket_subscriptions[websocket] = set()
        client_id = self._get_client_id(websocket)
        logger.info(f"WS Connected: {client_id}")

    def disconnect(self, websocket: WebSocket):
        client_id = self._get_client_id(websocket)

        if websocket in self.socket_subscriptions:
            subscribed_topics = self.socket_subscriptions[websocket]
            for topic in subscribed_topics:
                if topic in self.subscriptions and websocket in self.subscriptions[topic]:
                    self.subscriptions[topic].remove(websocket)
                    if not self.subscriptions[topic]:
                        del self.subscriptions[topic]
            
            del self.socket_subscriptions[websocket]
            logger.info(f"WS Disconnected: {client_id} (cleaned up)")

    async def subscribe(self, websocket: WebSocket, topic: str):
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        
        if websocket not in self.subscriptions[topic]:
            self.subscriptions[topic].append(websocket)
            self.socket_subscriptions[websocket].add(topic)
            logger.debug(f"WS Subscribe: {topic} (Client: {self._get_client_id(websocket)})")

    async def unsubscribe(self, websocket: WebSocket, topic: str):

        if topic in self.subscriptions and websocket in self.subscriptions[topic]:
            self.subscriptions[topic].remove(websocket)
            if not self.subscriptions[topic]:
                del self.subscriptions[topic]

        if websocket in self.socket_subscriptions:
            self.socket_subscriptions[websocket].discard(topic)
            
        logger.debug(f"WS Unsubscribe: {topic} (Client: {self._get_client_id(websocket)})")

    async def broadcast_to_topic(self, topic: str, raw_message: str):
        if topic in self.subscriptions:
            try:
                if not raw_message:  
                    return 
                
                payload_data = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Could not parse JSON for topic {topic}. Sending raw string.")
                payload_data = raw_message

            envelope = json.dumps({
                "topic": topic,
                "payload": payload_data 
            })
            
            active_connections = list(self.subscriptions[topic])

            for connection in active_connections:
                try:
                    await connection.send_text(envelope)
                except Exception:
                    logger.warning(f"WS Send failed. Disconnecting zombie client: {self._get_client_id(connection)}")
                    self.disconnect(connection)

websocket_manager = ConnectionManager()