import asyncio
import cloudpickle
import logging
import psutil
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def sample_task(a, b):
    return a + b

class PeerRegistry:
    def __init__(self):
        self.peers = {}
        self.last_heartbeat = {}
        self.load = {}

    def register(self, name, host, port):
        self.peers[name] = (host, port)
        self.last_heartbeat[name] = time.time()

    def update_heartbeat(self, name, cpu_load):
        self.last_heartbeat[name] = time.time()
        self.load[name] = cpu_load

    def alive_peers(self, timeout=5):
        now = time.time()
        return [name for name, ts in self.last_heartbeat.items() if now - ts < timeout]

    def lowest_load_peer(self, timeout=5):
        alive = self.alive_peers(timeout)
        if not alive:
            return None
        return min(alive, key=lambda n: self.load.get(n, 100))


registry = PeerRegistry()


async def handle_connection(reader, writer):
    data = await reader.read(65536)
    message = cloudpickle.loads(data)

    if message["type"] == "heartbeat":
        registry.update_heartbeat(message["name"], message["cpu"])
        logging.info(f"Heartbeat from {message['name']} (CPU: {message['cpu']}%)")

    elif message["type"] == "task":
        func, args = message["func"], message["args"]
        logging.info(f"Executing task from {message['sender']}: {func.__name__}{args}")
        try:
            result = {"status": "success", "result": func(*args)}
        except Exception as e:
            result = {"status": "error", "message": str(e)}
        writer.write(cloudpickle.dumps(result))
        await writer.drain()

    writer.close()


async def start_node(name, host, port):
    server = await asyncio.start_server(handle_connection, host, port)
    logging.info(f"Node '{name}' listening on {host}:{port}")
    async with server:
        await server.serve_forever()


async def send_heartbeat(name, target_host, target_port):
    while True:
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            reader, writer = await asyncio.open_connection(target_host, target_port)
            payload = cloudpickle.dumps({"type": "heartbeat", "name": name, "cpu": cpu})
            writer.write(payload)
            await writer.drain()
            writer.close()
        except Exception as e:
            logging.error(f"Heartbeat failed: {e}")
        await asyncio.sleep(2)


async def submit_task(sender_name, func, args, target_host, target_port):
    reader, writer = await asyncio.open_connection(target_host, target_port)
    payload = cloudpickle.dumps({"type": "task", "sender": sender_name, "func": func, "args": args})
    writer.write(payload)
    await writer.drain()

    data = await reader.read(65536)
    result = cloudpickle.loads(data)
    logging.info(f"Task result: {result}")
    writer.close()
    return result


async def demo():
    registry.register("node_A", "localhost", 9001)
    registry.register("node_B", "localhost", 9002)

    asyncio.create_task(start_node("node_A", "localhost", 9001))
    asyncio.create_task(start_node("node_B", "localhost", 9002))
    await asyncio.sleep(0.5)

    asyncio.create_task(send_heartbeat("node_A", "localhost", 9002))
    asyncio.create_task(send_heartbeat("node_B", "localhost", 9001))
    await asyncio.sleep(2.5)

    logging.info(f"Known alive peers: {registry.alive_peers()}")
    chosen = registry.lowest_load_peer()
    logging.info(f"Chosen peer for task (lowest CPU load): {chosen}")

    target_port = 9001 if chosen == "node_A" else 9002
    await submit_task("client", sample_task, (5, 7), "localhost", target_port)

    await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(demo())