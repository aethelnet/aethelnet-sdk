import torch
import logging
from typing import Callable, Any

logger = logging.getLogger("aethelnet.lgnn")

class TensorStream:
    """
    Aethelnet TensorStream.
    Wraps a SwarmNode and automatically converts incoming data payloads into PyTorch Tensors.
    """
    def __init__(self, node):
        self.node = node
        self._tensor_handlers = []
        
        # We hook into the raw node's TICK event and transform it.
        self.node.on("TICK", self._handle_raw_tick)
        self.node.on("COMPUTE_TENSOR", self._handle_compute)
        
    def on_tensor(self, handler: Callable[[torch.Tensor, dict], Any]):
        """
        Registers a callback that will receive fully prepared PyTorch tensors.
        """
        self._tensor_handlers.append(handler)
        
    async def _handle_raw_tick(self, payload: dict):
        """Converts a raw JSON tick into a physics-informed Tensor."""
        try:
            price = payload.get("price", 0.0)
            symbol = payload.get("symbol", "UNKNOWN")
            
            # Simple conversion for SDK v0.1
            # In a full implementation, we'd buffer N ticks and create a sequence tensor
            tensor_data = torch.tensor([[price]], dtype=torch.float32)
            
            context = {
                "symbol": symbol,
                "type": "TICK"
            }
            
            await self._dispatch_tensor(tensor_data, context)
        except Exception as e:
            logger.error(f"Failed to build tensor from TICK: {e}")

    async def _handle_compute(self, payload: dict):
        """Converts a federated training batch into Tensors."""
        try:
            raw_data = payload.get("data", [])
            topic = payload.get("topic", "UNKNOWN")
            
            if not raw_data:
                return
                
            # Create a 2D sequence tensor [batch_size, sequence_length]
            tensor_data = torch.tensor([raw_data], dtype=torch.float32)
            
            context = {
                "symbol": topic,
                "type": "COMPUTE_TENSOR"
            }
            
            await self._dispatch_tensor(tensor_data, context)
        except Exception as e:
            logger.error(f"Failed to build tensor from COMPUTE_TENSOR: {e}")
            
    async def _dispatch_tensor(self, tensor: torch.Tensor, context: dict):
        import asyncio
        for handler in self._tensor_handlers:
            if asyncio.iscoroutinefunction(handler):
                await handler(tensor, context)
            else:
                handler(tensor, context)
