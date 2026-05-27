from fastapi import APIRouter,WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse
import json
import logging
from app.adapters.geoflink.websocket_manager import websocket_manager
from app.adapters.geoflink import database


router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            
            try:
                command = json.loads(data)
                action = command.get("action")
                config_name = command.get("config_name")

                if action == "subscribe":
                    if config_name:

                        state = database.get_config_state(config_name)
                        if not state:
                            error_msg = {"status": "error", "message": f"Config '{config_name}' does not exist"}
                            await websocket.send_text(json.dumps(error_msg))
                            continue


                        target_topic = f"output_{config_name}"
                        await websocket_manager.subscribe(websocket, target_topic)
                        await websocket.send_text(json.dumps({"status": "subscribed", "topic": target_topic}))
                    else:
                        await websocket.send_text(json.dumps({"error": "Missing config_name"}))
                elif action == "unsubscribe":
                    if config_name:
                        target_topic = f"output_{config_name}"
                        await websocket_manager.unsubscribe(websocket, target_topic)
                        await websocket.send_text(json.dumps({"status": "unsubscribed", "topic": target_topic}))
                    else:
                        await websocket.send_text(json.dumps({"error": "Missing config_name"}))
                
                elif action == "ping":
                    await websocket.send_text(json.dumps({"pong": True}))

            except json.JSONDecodeError:
                logger.warning("Received invalid JSON from client")
                await websocket.send_text(json.dumps({"error": "Invalid JSON format"}))
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WS Error: {e}")
        websocket_manager.disconnect(websocket)


@router.get("/ws", tags=["Geoflink: Websocket"], summary="WebSocket Protocol Documentation")
def websocket_documentation():
    """
    **This endpoint requires a WebSocket connection.**
    
    Do not make HTTP GET requests here. Connect using a WebSocket client (e.g., Postman, JS).

    ### **Connection URL:**
    `ws://<host>:<port>/geoflink/ws`

    ---

    ### **Supported Commands (JSON Payload):**

    #### **1. Subscribe to a configuration stream**
    Start receiving real-time alerts for a specific job configuration.
    
    **Request:**
    ```json
    {
        "action": "subscribe",
        "config_name": "your_config_name"
    }
    ```
    **Response (Success):**
    ```json
    {
        "status": "subscribed",
        "topic": "output_your_config_name"
    }
    ```

    #### **2. Unsubscribe from a stream**
    Stop receiving alerts for a specific configuration.
    
    **Request:**
    ```json
    {
        "action": "unsubscribe",
        "config_name": "your_config_name"
    }
    ```
    **Response (Success):**
    ```json
    {
        "status": "unsubscribed",
        "topic": "output_your_config_name"
    }
    ```

    #### **3. Heartbeat (Ping)**
    Keep the connection alive and prevent idle timeouts.
    
    **Request:**
    ```json
    {
        "action": "ping"
    }
    ```
    **Response:**
    ```json
    {"pong": true}
    ```

    ---

    ### **Error Responses:**
    If parameters are missing or JSON is invalid:
    ```json
    {
        "error": "Missing config_name"
    }
    ```
    """
    return JSONResponse(
        content={
            "error": "Upgrade Required",
            "message": "This endpoint requires a WebSocket connection. Please use ws:// protocol."
        },
        status_code=status.HTTP_426_UPGRADE_REQUIRED
    )