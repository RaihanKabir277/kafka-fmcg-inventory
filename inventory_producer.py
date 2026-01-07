import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "inventory.movements")


DEPOTS = ["DEPOT_DHK_01", "DEPOT_CTG_01"]
DISTRIBUTORS = ["DIST_PRAN_01", "DIST_NESTLE_02"]
SKUS = ["SKU_PRAN_JUICE_250ML", "SKU_NESTLE_MILK_1L"]


# Business-like movement types:
# - INBOUND: stock increases
# - OUTBOUND: stock decreases
# - RETURN: stock increases
# - ADJUSTMENT: can be + or - (cycle count / reconciliation), but keep small
MOVEMENT_TYPES = ["INBOUND", "OUTBOUND", "RETURN", "ADJUSTMENT"]




def _utc_iso():
   return datetime.now(timezone.utc).isoformat()




def create_inventory_event(event_type=None, distributor_id=None, sku_code=None, depot_id=None):
   now = datetime.now(timezone.utc)
   event_type = event_type or random.choices(
       population=MOVEMENT_TYPES,
       weights=[0.45, 0.40, 0.10, 0.05],   # more inbound/outbound, fewer adjustments
       k=1
   )[0]


   depot_id = depot_id or random.choice(DEPOTS)
   distributor_id = distributor_id or random.choice(DISTRIBUTORS)
   sku_code = sku_code or random.choice(SKUS)


   # quantity rules by type (more realistic than -100..500 always)
   if event_type == "INBOUND":
       quantity = random.randint(80, 300)           # always +
   elif event_type == "OUTBOUND":
       quantity = -random.randint(20, 180)          # always -
   elif event_type == "RETURN":
       quantity = random.randint(5, 60)             # always +
   else:  # ADJUSTMENT
       quantity = random.choice([-1, 1]) * random.randint(1, 25)  # small +/- only


   return {
       "event_id": str(uuid.uuid4()),
       "event_type": event_type,
       "event_ts": now.isoformat(),
       "depot_id": depot_id,
       "distributor_id": distributor_id,
       "sku_code": sku_code,
       "batch_no": f"BATCH_{now.strftime('%Y%m%d')}_A",
       "quantity": quantity,
       "source_system": "ERP"
   }




def send_opening_stock(producer):
   # Send a baseline so dashboard doesn't start with 0 then instantly go negative from sales
   print("Sending opening stock baseline...")
   for dist in DISTRIBUTORS:
       for sku in SKUS:
           opening_qty = random.randint(300, 900)
           e = create_inventory_event(
               event_type="INBOUND",
               distributor_id=dist,
               sku_code=sku
           )
           e["quantity"] = opening_qty
           e["event_type"] = "INBOUND"
           producer.send(TOPIC, value=e)
           print("Opening stock:", e)
   producer.flush()




def main():
   producer = KafkaProducer(
       bootstrap_servers=BROKER,
       value_serializer=lambda v: json.dumps(v).encode("utf-8"),
   )


   print(f"Producing inventory events to '{TOPIC}' on broker '{BROKER}'...")
   send_opening_stock(producer)


   while True:
       event = create_inventory_event()
       producer.send(TOPIC, value=event)
       print("Sent:", event)
       time.sleep(2)  # keep slower than sales




if __name__ == "__main__":
   main()







