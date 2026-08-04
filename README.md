# Aethelnet SDK

The official Python SDK for interacting with the Aethelnet Liquid Graph Engine and P2P Swarm.

## Installation

```bash
pip install aethelnet
```

## Quickstart

```python
from aethelnet.swarm import SwarmNode
from aethelnet.lgnn import TensorStream

# Connect to the mesh
node = SwarmNode(api_key="YOUR_KEY", mode="satellite")

# Stream data through the Liquid Graph
stream = TensorStream(node)

@stream.on_data
def process(data):
    confidence = node.lgnn.evaluate(data)
    print(f"Match confidence: {confidence}%")
```
