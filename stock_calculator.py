import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone


from kafka import KafkaConsumer, KafkaProducer


# ----------------------------
# CONFIG (override via env if needed)
# ----------------------------
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")


SALES_TOPIC = os.getenv("SALES_TOPIC", "sales.events")
INV_TOPIC = os.getenv("INV_TOPIC", "inventory.movements")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order.events")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alerts.lowstock")


LOW_STOCK_THRESHOLD = int(os.getenv("LOW_STOCK_THRESHOLD", "50"))
ALERT_DEBOUNCE_SEC = int(os.getenv("ALERT_DEBOUNCE_SEC", "30"))


PRINT_EVERY_SEC = float(os.getenv("PRINT_EVERY_SEC", "2.0"))  # table refresh rate
MAX_ROWS = int(os.getenv("MAX_ROWS", "10"))




# ----------------------------
# HELPERS
# ----------------------------
def safe_int(x, default=0):
   try:
       return int(x)
   except Exception:
       return default




def utc_iso_now():
   return datetime.now(timezone.utc).isoformat()




def create_consumer(topic, group_id="stock-calculator-group"):
   return KafkaConsumer(
       topic,
       bootstrap_servers=BROKER,
       auto_offset_reset="earliest",
       enable_auto_commit=True,
       value_deserializer=lambda v: json.loads(v.decode("utf-8")),
       group_id=group_id,
   )




def status_for(stock, initialized: bool):
   # Only call something LOW/OOS if it has been initialized by inventory
   if not initialized:
       return "UNINIT"
   if stock <= 0:
       return "OOS"
   if stock < LOW_STOCK_THRESHOLD:
       return "LOW"
   return "OK"




def print_dashboard(stock_state, sales_state, order_state, initialized_pairs, max_rows=10):
   keys = set(stock_state.keys()) | set(sales_state.keys()) | set(order_state.keys())


   def row_tuple(k):
       dist, sku = k
       st = stock_state.get(k, 0)
       sl = sales_state.get(k, 0)
       od = order_state.get(k, 0)
       init = k in initialized_pairs
       stt = status_for(st, init)


       priority = {"OOS": 0, "LOW": 1, "OK": 2, "UNINIT": 3}
       return (priority.get(stt, 9), st, dist or "-", sku or "-", st, sl, od, stt)


   rows = [row_tuple(k) for k in keys]
   rows.sort(key=lambda r: (r[0], r[1]))  # status priority, then lowest stock


   print("\n=== Stock vs Sales vs Orders (Top rows) ===")
   print(f"{'Distributor':15} {'SKU':25} {'Stock':>8} {'Sales':>8} {'Orders':>8} {'Status':>8}")
   print("-" * 82)


   for r in rows[:max_rows]:
       _, _, dist, sku, st, sl, od, stt = r
       print(f"{dist:15} {sku:25} {st:8d} {sl:8d} {od:8d} {stt:>8}")




# ----------------------------
# MAIN
# ----------------------------
def main():
   sales_consumer = create_consumer(SALES_TOPIC)
   inv_consumer = create_consumer(INV_TOPIC)
   order_consumer = create_consumer(ORDER_TOPIC)


   # State
   stock_state = defaultdict(int)  # (dist, sku) -> current stock
   sales_state = defaultdict(int)  # (dist, sku) -> cumulative sales qty
   order_state = defaultdict(int)  # (dist, sku) -> cumulative order qty


   # Only consider LOW/OOS "real" after we have seen inventory for the pair
   initialized_pairs = set()


   # Alert debounce
   last_alert_ts = {}  # (dist,sku) -> epoch seconds


   alert_producer = KafkaProducer(
       bootstrap_servers=BROKER,
       value_serializer=lambda v: json.dumps(v).encode("utf-8"),
       # Optional (more reliable delivery at cost of latency)
       # acks="all",
   )


   print("Starting real-time Stock vs Sales vs Orders calculator...")
   print(f"Broker: {BROKER}")
   print(f"Topics: inv={INV_TOPIC}, sales={SALES_TOPIC}, orders={ORDER_TOPIC}, alerts={ALERT_TOPIC}")
   print(f"LOW_STOCK_THRESHOLD={LOW_STOCK_THRESHOLD}, ALERT_DEBOUNCE_SEC={ALERT_DEBOUNCE_SEC}")


   last_print = 0.0


   def maybe_send_alert(dist, sku, current_stock):
       key = (dist, sku)
       now = time.time()
       last = last_alert_ts.get(key, 0)
       if now - last < ALERT_DEBOUNCE_SEC:
           return
       last_alert_ts[key] = now


       alert = {
           "event_type": "LOW_STOCK",
           "event_ts": utc_iso_now(),
           "distributor_id": dist,
           "sku_code": sku,
           "current_stock": current_stock,
           "threshold": LOW_STOCK_THRESHOLD,
           "source_system": "STOCK_ENGINE",
       }
       alert_producer.send(ALERT_TOPIC, alert)


   try:
       while True:
           inv_records = inv_consumer.poll(timeout_ms=500)
           sales_records = sales_consumer.poll(timeout_ms=500)
           order_records = order_consumer.poll(timeout_ms=500)


           # 1) inventory.movements
           for _, messages in inv_records.items():
               for msg in messages:
                   event = msg.value or {}
                   dist = event.get("distributor_id")
                   sku = event.get("sku_code")
                   if not dist or not sku:
                       continue


                   qty = safe_int(event.get("quantity", 0))
                   key = (dist, sku)


                   stock_state[key] += qty
                   initialized_pairs.add(key)


                   # alert only for initialized pairs (always true here)
                   if stock_state[key] < LOW_STOCK_THRESHOLD:
                       maybe_send_alert(dist, sku, stock_state[key])


           # 2) sales.events
           for _, messages in sales_records.items():
               for msg in messages:
                   event = msg.value or {}
                   dist = event.get("distributor_id")
                   sku = event.get("sku_code")
                   if not dist or not sku:
                       continue


                   qty = safe_int(event.get("quantity", 0))
                   key = (dist, sku)


                   stock_state[key] -= qty
                   sales_state[key] += qty


                   # only alert if we have stock baseline (prevents fake low-stock from uninitialized pairs)
                   if key in initialized_pairs and stock_state[key] < LOW_STOCK_THRESHOLD:
                       maybe_send_alert(dist, sku, stock_state[key])


           # 3) order.events
           for _, messages in order_records.items():
               for msg in messages:
                   event = msg.value or {}
                   dist = event.get("distributor_id")
                   sku = event.get("sku_code")
                   if not dist or not sku:
                       continue


                   qty = safe_int(event.get("quantity", 0))
                   key = (dist, sku)
                   order_state[key] += qty


           # 4) print a readable snapshot every PRINT_EVERY_SEC
           now = time.time()
           if now - last_print >= PRINT_EVERY_SEC:
               print_dashboard(stock_state, sales_state, order_state, initialized_pairs, max_rows=MAX_ROWS)
               last_print = now


               # Ensures alerts are pushed before long idle/exit; flush blocks until prior sends complete. [web:85]
               alert_producer.flush()


   except KeyboardInterrupt:
       print("\nStopping stock-calculator...")
   finally:
       try:
           alert_producer.flush()
           alert_producer.close()
       except Exception:
           pass
       try:
           sales_consumer.close()
           inv_consumer.close()
           order_consumer.close()
       except Exception:
           pass




if __name__ == "__main__":
   main()







