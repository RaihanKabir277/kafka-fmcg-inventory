import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "order.events"

DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02"]
SKUS = ["SKU_PRAN_JUICE_250ML", "SKU_NESTLE_MILK_1L"]

def create_order_event():
    now = datetime.now(timezone.utc)
    return {
        "event_type": "ORDER_PLACED",
        "event_ts": now.isoformat(),
        "distributor_id": random.choice(DISTRIBUTORS),
        "order_id": f"ORD_{uuid.uuid4().hex[:8]}",
        "sku_code": random.choice(SKUS),
        "quantity": random.randint(10, 200),
        "status": "PENDING",
        "source_system": "DMS"
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"Producing order events to '{TOPIC}'...")
    while True:
        event = create_order_event()
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(3)

if __name__ == "__main__":
    main()


import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "order.events"

DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02"]
SKUS = ["SKU_PRAN_JUICE_250ML", "SKU_NESTLE_MILK_1L"]

def create_order_event():
    now = datetime.now(timezone.utc)
    return {
        "event_type": "ORDER_PLACED",
        "event_ts": now.isoformat(),
        "distributor_id": random.choice(DISTRIBUTORS),
        "order_id": f"ORD_{uuid.uuid4().hex[:8]}",
        "sku_code": random.choice(SKUS),
        "quantity": random.randint(10, 200),
        "status": "PENDING",
        "source_system": "DMS"
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"Producing order events to '{TOPIC}'...")
    while True:
        event = create_order_event()
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(3)

if __name__ == "__main__":
    main()


