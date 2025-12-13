import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "inventory.movements"

DEPOTS = ["DEPOT_DHK_01", "DEPOT_CTG_01"]
DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02"]
SKUS = ["SKU_PRAN_JUICE_250ML", "SKU_NESTLE_MILK_1L"]

MOVEMENT_TYPES = ["INBOUND", "OUTBOUND", "RETURN", "ADJUSTMENT"]

def create_inventory_event():
    now = datetime.now(timezone.utc)
    return {
        "event_type": random.choice(MOVEMENT_TYPES),
        "event_ts": now.isoformat(),
        "depot_id": random.choice(DEPOTS),
        "distributor_id": random.choice(DISTRIBUTORS),
        "sku_code": random.choice(SKUS),
        "batch_no": f"BATCH_{now.strftime('%Y%m%d')}_A",
        "quantity": random.randint(-100, 500),  # negative=outbound
        "source_system": "ERP"
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"Producing inventory events to '{TOPIC}'...")
    while True:
        event = create_inventory_event()
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(2)  # slower than sales

if __name__ == "__main__":
    main()


