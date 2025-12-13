import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "sales.events"

DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02", "DIST_ACI_03", "DIST_AKIJ_04"]
DEPOTS = ["DEPOT_DHK_01", "DEPOT_CTG_01", "DEPOT_KHL_01"]
REGIONS = ["DHAKA_NORTH", "DHAKA_SOUTH", "CTG_CITY", "KHULNA"]
CHANNELS = ["DMS", "POS", "MOBILE_APP"]
SKUS = [
    ("SKU_PRAN_JUICE_250ML", 25.0),
    ("SKU_NESTLE_MILK_1L", 90.0),
    ("SKU_ACI_SALT_1KG", 35.0),
    ("SKU_AKIJ_SOFTDRINK_500ML", 40.0),
]

def create_sale_event():
    distributor_id = random.choice(DISTRIBUTORS)
    depot_id = random.choice(DEPOTS)
    region = random.choice(REGIONS)
    channel = random.choice(CHANNELS)
    sku_code, unit_price = random.choice(SKUS)

    quantity = random.randint(1, 50)
    now = datetime.now(timezone.utc)

    order_id = f"ORD_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"
    invoice_id = f"INV_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"

    event = {
        "event_type": "SALE",
        "event_ts": now.isoformat(),
        "distributor_id": distributor_id,
        "depot_id": depot_id,
        "region": region,
        "channel": channel,
        "sales_rep_id": f"SR_{random.randint(1000, 9999)}",
        "order_id": order_id,
        "invoice_id": invoice_id,
        "sku_code": sku_code,
        "batch_no": f"BATCH_{now.strftime('%Y%m%d')}",
        "quantity": quantity,
        "unit_price": unit_price,
        "currency": "BDT",
        "source_system": channel,
    }

    return event

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing sales events to topic '{TOPIC}' on {BROKER} ...")
    while True:
        event = create_sale_event()
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        # small delay so you can see events streaming in
        time.sleep(1)

if __name__ == "__main__":
    main()


