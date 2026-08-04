import asyncio
import logging
from typing import Optional, Callable

logger = logging.getLogger("aethelnet.swarm")

class SwarmNode:
    """
    Aethelnet Swarm Node.
    Connects to the Aethelnet P2P Mesh and handles decentralized communication.
    """
    def __init__(self, api_key: str = None, mode: str = "satellite", prime_url: str = "ws://127.0.0.1:8000/ws/swarm"):
        self.api_key = api_key
        self.mode = mode
        self.prime_url = prime_url
        self.connected = False
        
        self.ws = None
        self._handlers = {}
        logger.info(f"Initialized SwarmNode in '{self.mode}' mode, targeting {self.prime_url}")
        
    def on(self, event_type: str, handler: Callable):
        """Register a callback for a specific swarm event."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        
    async def connect(self):
        """Connects to the Aethelnet Prime node asynchronously."""
        import websockets
        import json
        
        self.connected = True
        logger.info(f"Connecting to {self.prime_url}...")
        
        try:
            async with websockets.connect(self.prime_url) as ws:
                self.ws = ws
                # Identity Handshake
                await ws.send(json.dumps({
                    "type": "JOIN_SWARM",
                    "node_id": f"sdk-node-{id(self)}",
                    "capabilities": [self.mode]
                }))
                
                logger.info("Successfully joined the Aethelnet swarm.")
                
                # Message Loop
                async for message in ws:
                    data = json.loads(message)
                    msg_type = data.get("type")
                    payload = data.get("payload", data)
                    
                    if msg_type in self._handlers:
                        for handler in self._handlers[msg_type]:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(payload)
                            else:
                                handler(payload)
                                
        except Exception as e:
            self.connected = False
            logger.error(f"Connection lost: {e}")
            
    async def emit(self, msg_type: str, payload: dict):
        """Sends data back into the swarm."""
        import json
        if self.ws and self.connected:
            await self.ws.send(json.dumps({
                "type": msg_type,
                "node_id": f"sdk-node-{id(self)}",
                "payload": payload
            }))
        
    def disconnect(self):
        """Gracefully disconnects from the swarm."""
        self.connected = False
        if self.ws:
            asyncio.create_task(self.ws.close())
        logger.info("Disconnected from swarm.")
