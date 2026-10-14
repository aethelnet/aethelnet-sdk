import asyncio
import logging
import time
import uuid

logger = logging.getLogger("aethelnet.osint")


class Hunter:
    """
    Aethelnet OSINT Hunter.
    Allows external nodes to gather data from the web (Twitter, News, Telegram)
    and submit their "convictions" or raw data back to the Swarm.
    """
    def __init__(self, node):
        self.node = node

    async def submit_signal(self, source: str, raw_text: str, confidence: float = 1.0, metadata: dict = None) -> dict:
        """
        Submit a piece of OSINT data to the Aethelnet Mainnet.
        """
        payload = {
            "source": source,
            "raw_text": raw_text,
            "confidence": max(0.0, min(1.0, float(confidence))),
            "metadata": metadata or {},
            "timestamp": time.time(),
        }

        logger.info(f"Submitting OSINT signal from '{source}' to Swarm...")
        await self.node.emit("OSINT_CONVICTION", payload)
        return payload

    async def request_verification(self, claim: str, timeout: float = 10.0) -> dict:
        """
        Ask the Swarm to verify a specific claim and await the consensus response.
        Emits a VERIFY_CLAIM event with a correlation request_id and resolves when
        a matching VERIFY_RESPONSE event arrives.
        """
        if not claim or not claim.strip():
            raise ValueError("claim must be a non-empty string")

        request_id = f"verify-{uuid.uuid4().hex[:12]}"
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _on_verify_response(payload: dict):
            if isinstance(payload, dict) and payload.get("request_id") == request_id and not future.done():
                future.set_result(payload)

        self.node.on("VERIFY_RESPONSE", _on_verify_response)
        try:
            await self.node.emit(
                "VERIFY_CLAIM",
                {
                    "request_id": request_id,
                    "claim": claim.strip(),
                    "timestamp": time.time(),
                },
            )
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            handlers = self.node._handlers.get("VERIFY_RESPONSE", [])
            if _on_verify_response in handlers:
                handlers.remove(_on_verify_response)

