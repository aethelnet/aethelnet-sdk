from collections import defaultdict, deque
import logging
from typing import Callable, Any, Deque, Dict
import torch

logger = logging.getLogger("aethelnet.lgnn")


class TensorStream:
    """
    Aethelnet TensorStream.
    Wraps a SwarmNode and converts incoming tick and compute payloads into PyTorch Tensors,
    maintaining a per-symbol rolling sequence buffer of length `window_size`.
    """
    def __init__(self, node, window_size: int = 1):
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self.node = node
        self.window_size = int(window_size)
        self._tensor_handlers = []
        self._tick_buffers: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window_size)
        )

        # Hook into the raw node's TICK and COMPUTE_TENSOR events
        self.node.on("TICK", self._handle_raw_tick)
        self.node.on("COMPUTE_TENSOR", self._handle_compute)

    def on_tensor(self, handler: Callable[[torch.Tensor, dict], Any]):
        """
        Registers a callback that will receive prepared PyTorch tensors.
        """
        self._tensor_handlers.append(handler)

    async def _handle_raw_tick(self, payload: dict):
        """Appends a raw JSON tick to the symbol's rolling window and emits a [1, seq_len] Tensor."""
        try:
            price = float(payload.get("price", 0.0))
            symbol = str(payload.get("symbol", "UNKNOWN"))

            buf = self._tick_buffers[symbol]
            buf.append(price)
            tensor_data = torch.tensor([list(buf)], dtype=torch.float32)

            context = {
                "symbol": symbol,
                "type": "TICK",
                "window_size": self.window_size,
                "seq_len": len(buf),
            }

            await self._dispatch_tensor(tensor_data, context)
        except Exception as e:
            logger.error(f"Failed to build tensor from TICK: {e}")

    async def _handle_compute(self, payload: dict):
        """Converts a federated training batch into a 2D Tensor."""
        try:
            raw_data = payload.get("data", [])
            topic = payload.get("topic", "UNKNOWN")

            if not raw_data:
                return

            tensor_data = torch.tensor([raw_data], dtype=torch.float32)
            if tensor_data.ndim > 2:
                tensor_data = tensor_data.squeeze(0)

            context = {
                "symbol": topic,
                "type": "COMPUTE_TENSOR",
            }

            await self._dispatch_tensor(tensor_data, context)
        except Exception as e:
            logger.error(f"Failed to build tensor from COMPUTE_TENSOR: {e}")

    async def _dispatch_tensor(self, tensor: torch.Tensor, context: dict):
        import inspect
        for handler in self._tensor_handlers:
            if inspect.iscoroutinefunction(handler):
                await handler(tensor, context)
            else:
                handler(tensor, context)

