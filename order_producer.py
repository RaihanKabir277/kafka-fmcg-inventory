import json
import os
import random
import time
import uuid
from datetime import datetime, timezone


from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "order.events")


DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02"]
SKUS = ["SKU_PRAN_JUICE_250ML", "SKU_NESTLE_MILK_1L"]


# Optional: make orders more "enterprise-like" by having statuses
STATUSES = ["PENDING", "CONFIRMED", "PENDING"]  # weighted by repetition




def create_order_event():
   now = datetime.now(timezone.utc)


   # More realistic order quantities: mostly smaller, sometimes bigger
   qty = random.choices(
       population=[10, 20, 30, 50, 80, 120, 200],
       weights=[18, 18, 16, 16, 14, 10, 8],
       k=1
   )[0]


   return {
       "event_id": str(uuid.uuid4()),
       "event_type": "ORDER_PLACED",
       "event_ts": now.isoformat(),
       "distributor_id": random.choice(DISTRIBUTORS),
       "order_id": f"ORD_{uuid.uuid4().hex[:8]}",
       "sku_code": random.choice(SKUS),
       "quantity": qty,
       "status": random.choice(STATUSES),
       "source_system": "DMS",
   }




def main():
   producer = KafkaProducer(
       bootstrap_servers=BROKER,
       value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # JSON -> bytes [web:65]
   )


   print(f"Producing order events to '{TOPIC}' on broker '{BROKER}'...")
   try:
       while True:
           event = create_order_event()
           producer.send(TOPIC, value=event)
           print("Sent:", event)
           time.sleep(3)
   except KeyboardInterrupt:
       print("\nStopping order producer...")
   finally:
       # Ensures previously sent records complete before exit. [web:85]
       try:
           producer.flush()
           producer.close()
       except Exception:
           pass




if __name__ == "__main__":
   main()







