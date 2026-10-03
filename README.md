# MeshWeaver

A decentralized, peer-to-peer task queue for distributed and edge computing, built in pure Python.

## Problem

Traditional distributed task queues (like Celery) require a central message broker (Redis, RabbitMQ). For edge computing — hundreds of devices like Raspberry Pis deployed in the field — a central broker is a single point of failure and adds configuration overhead.

## Approach

MeshWeaver removes the central server using a peer-to-peer mesh network. Nodes discover each other, report their load, and tasks are routed to the least busy available node. If a node goes offline, its task can be reassigned.

## Components

### 1. Task Serializer (`serializer.py` + `executor.py`)
Core execution layer: serializes a Python function and its arguments using `cloudpickle`, sends them over a TCP socket, executes them remotely, and returns the result.

**Run it:**
```bash
# Terminal 1
python3 executor.py

# Terminal 2
python3 serializer.py
```

### 2. Mesh Networking Demo (`mesh_node.py`)
Demonstrates peer-to-peer node communication, heartbeat-based liveness tracking, and load-based task routing using `asyncio`.

**Run it:**
```bash
python3 mesh_node.py
```

## Tech Stack
- Python 3.12
- `cloudpickle` — function/argument serialization
- `asyncio` — peer-to-peer networking
- `psutil` — real CPU load reporting

## Status
- ✅ Task serialization and remote execution — complete
- ✅ Simplified peer discovery, heartbeat, load-based routing — working demo
- ⬜ Full Kademlia DHT — not implemented (simplified peer registry used instead)
- ⬜ Automatic task reassignment on node failure — not yet implemented
