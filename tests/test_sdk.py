import asyncio
import unittest
from unittest.mock import patch

import torch

from aethelnet import SwarmNode, TensorStream, Hunter
from aethelnet.osint.proxy_manager import ProxyManager


class _FakeResponse:
    def __init__(self, status_code: int, body: str = "ok"):
        self.status_code = status_code
        self.text = body


class TestAethelnetSDK(unittest.TestCase):
    def test_tensor_stream_rolling_window_and_compute_batch(self):
        """Verifies TensorStream maintains per-symbol rolling windows and converts compute batches."""
        node = SwarmNode(mode="satellite")
        stream = TensorStream(node, window_size=3)

        received = []
        stream.on_tensor(lambda t, ctx: received.append((t.clone(), dict(ctx))))

        async def _run():
            for p in (100.0, 101.5, 103.0, 104.5):
                for h in node._handlers["TICK"]:
                    await h({"symbol": "ETH", "price": p})
            for h in node._handlers["COMPUTE_TENSOR"]:
                await h({"topic": "LGNN_BATCH", "data": [1.0, 2.0, 3.0, 4.0]})

        asyncio.run(_run())

        self.assertEqual(len(received), 5)
        # Tick 1 -> shape (1, 1)
        self.assertEqual(tuple(received[0][0].shape), (1, 1))
        # Tick 3 -> shape (1, 3): [100.0, 101.5, 103.0]
        self.assertTrue(torch.allclose(received[2][0], torch.tensor([[100.0, 101.5, 103.0]])))
        # Tick 4 -> rolling window drops 100.0 -> [101.5, 103.0, 104.5]
        self.assertTrue(torch.allclose(received[3][0], torch.tensor([[101.5, 103.0, 104.5]])))
        # Compute batch
        self.assertEqual(received[4][1]["type"], "COMPUTE_TENSOR")
        self.assertTrue(torch.allclose(received[4][0], torch.tensor([[1.0, 2.0, 3.0, 4.0]])))

    def test_hunter_submit_and_request_verification_roundtrip(self):
        """Verifies Hunter clamps confidence and resolves request_verification via VERIFY_RESPONSE."""
        node = SwarmNode(mode="osint")
        emitted = []

        async def _fake_emit(msg_type: str, payload: dict):
            emitted.append((msg_type, payload))
            if msg_type == "VERIFY_CLAIM":
                # Simulate asynchronous swarm oracle consensus reply
                req_id = payload["request_id"]
                for h in list(node._handlers.get("VERIFY_RESPONSE", [])):
                    h({
                        "request_id": req_id,
                        "verified": True,
                        "consensus_score": 0.94,
                        "claim": payload["claim"],
                    })

        node.emit = _fake_emit
        hunter = Hunter(node)

        async def _run():
            sig = await hunter.submit_signal("reuters", "Grid frequency stable", confidence=1.7)
            self.assertEqual(sig["confidence"], 1.0)
            res = await hunter.request_verification("Benin Expedition 1897 provenance match", timeout=1.0)
            return res

        result = asyncio.run(_run())
        self.assertTrue(result["verified"])
        self.assertAlmostEqual(result["consensus_score"], 0.94)
        self.assertEqual(emitted[0][0], "OSINT_CONVICTION")
        self.assertEqual(emitted[1][0], "VERIFY_CLAIM")
        # Handler must be cleaned up after resolution
        self.assertEqual(len(node._handlers.get("VERIFY_RESPONSE", [])), 0)

    def test_proxy_manager_retries_on_429_and_succeeds(self):
        """Verifies ProxyManager retries rate-limited (429) responses without hitting external network."""
        manager = ProxyManager(max_retries=3, base_delay=0.01)
        attempts = []

        def _flaky_request(headers):
            self.assertIn("User-Agent", headers)
            attempts.append(headers["User-Agent"])
            if len(attempts) < 3:
                return _FakeResponse(429, "Too Many Requests")
            return _FakeResponse(200, "Success payload")

        with patch("time.sleep"):
            resp = manager.execute_with_resilience(_flaky_request)

        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(attempts), 3)


if __name__ == "__main__":
    unittest.main()
