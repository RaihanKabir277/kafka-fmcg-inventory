import json
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
SALES_TOPIC = "sales.events"
INV_TOPIC = "inventory.movements"
ORDER_TOPIC = "order.events"
ALERT_TOPIC = "alerts.lowstock"

LOW_STOCK_THRESHOLD = 50  # you can tune this later

def create_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="stock-calculator-group",
    )

def main():
    # 1) One consumer per topic
    sales_consumer = create_consumer(SALES_TOPIC)
    inv_consumer = create_consumer(INV_TOPIC)
    order_consumer = create_consumer(ORDER_TOPIC)

    # 2) State stores
    stock_state = defaultdict(int)
    sales_state = defaultdict(int)
    order_state = defaultdict(int)

    # 3) Producer for alerts
    alert_producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting real-time Stock vs Sales vs Orders calculator...")

    while True:
        inv_records = inv_consumer.poll(timeout_ms=500)
        sales_records = sales_consumer.poll(timeout_ms=500)
        order_records = order_consumer.poll(timeout_ms=500)

        # 4) Process inventory.movements
        for tp, messages in inv_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                stock_state[key] += qty

                print("[INV] ", key, "qty_change:", qty, "stock:", stock_state[key])

                # low-stock check after stock update
                if stock_state[key] < LOW_STOCK_THRESHOLD:
                    alert = {
                        "event_type": "LOW_STOCK",
                        "event_ts": datetime.now(timezone.utc).isoformat(),
                        "distributor_id": distributor_id,
                        "sku_code": sku_code,
                        "current_stock": stock_state[key],
                        "threshold": LOW_STOCK_THRESHOLD,
                        "source_system": "STOCK_ENGINE",
                    }
                    alert_producer.send(ALERT_TOPIC, alert)
                    print("[ALERT] Sent:", alert)

        # 5) Process sales.events
        for tp, messages in sales_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                # stock goes down by sales
                stock_state[key] -= qty
                sales_state[key] += qty

                print("[SALE]", key, "qty_change:", -qty, "stock:", stock_state[key])

                if stock_state[key] < LOW_STOCK_THRESHOLD:
                    alert = {
                        "event_type": "LOW_STOCK",
                        "event_ts": datetime.now(timezone.utc).isoformat(),
                        "distributor_id": distributor_id,
                        "sku_code": sku_code,
                        "current_stock": stock_state[key],
                        "threshold": LOW_STOCK_THRESHOLD,
                        "source_system": "STOCK_ENGINE",
                    }
                    alert_producer.send(ALERT_TOPIC, alert)
                    print("[ALERT] Sent:", alert)

        # 6) Process order.events
        for tp, messages in order_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                order_state[key] += qty

                print("[ORD] ", key, "order_qty+:", qty, "total_orders:", order_state[key])

        # 7) Optional: print combined “Stock vs Sales vs Orders” snapshot
        for key in list(stock_state.keys())[:5]:  # only first few keys to avoid spam
            dist, sku = key
            print(
                "[SSO] DIST:", dist,
                "SKU:", sku,
                "Stock:", stock_state[key],
                "Sales:", sales_state[key],
                "Orders:", order_state[key],
            )

if __name__ == "__main__":
    main()




