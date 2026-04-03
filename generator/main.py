import asyncio
import random
import uuid
from datetime import datetime, timezone

import ujson
from aiokafka import AIOKafkaProducer
from faker import Faker

KAFKA_TOPIC = "clickstream"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
EVENTS_PER_SECOND = 500

fake = Faker()

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGES = [
    "/",
    "/catalog",
    "/product/101",
    "/product/205",
    "/cart",
    "/checkout",
]
EVENT_WEIGHTS = [
    ("page_view", 0.58),
    ("view_product", 0.22),
    ("add_to_cart", 0.14),
    ("purchase", 0.06),
]


def pick_event_type() -> str:
    roll = random.random()
    cumulative = 0.0

    for event_name, weight in EVENT_WEIGHTS:
        cumulative += weight
        if roll <= cumulative:
            return event_name

    return "page_view"


def build_url(event_type: str) -> str:
    if event_type == "purchase":
        return "/checkout"
    if event_type == "add_to_cart":
        return "/cart"
    return random.choice(PAGES)


def generate_event() -> dict:
    event_type = pick_event_type()

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 15000),
        "session_id": fake.uuid4(),
        "event_type": event_type,
        "url": build_url(event_type),
        "device_type": random.choice(DEVICE_TYPES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


async def send_events(producer: AIOKafkaProducer) -> None:
    while True:
        started_at = asyncio.get_running_loop().time()
        send_tasks = []

        for _ in range(EVENTS_PER_SECOND):
            payload = ujson.dumps(generate_event()).encode("utf-8")
            send_tasks.append(producer.send_and_wait(KAFKA_TOPIC, value=payload))

        await asyncio.gather(*send_tasks)

        elapsed = asyncio.get_running_loop().time() - started_at
        print(f"sent {EVENTS_PER_SECOND} events in {elapsed:.3f}s")

        if elapsed < 1:
            await asyncio.sleep(1 - elapsed)


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await producer.start()
    try:
        print(f"generator started, target rate = {EVENTS_PER_SECOND} eps")
        await send_events(producer)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
