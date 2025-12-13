import json
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from stream_processing.db_writer import upsert_snapshot_rows  # Step 9 hook

BROKER = "localhost:9092"
SALES_TOPIC = "sales.events"
INV_TOPIC = "inventory.movements"
ORDER_TOPIC = "order.events"
ALERT_TOPIC = "alerts.lowstock"

LOW_STOCK_THRESHOLD = 50  # tune as needed


def create_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="stock-calculator-group",
    )


def print_dashboard(stock_state, sales_state, order_state, max_rows=10):
    print("\n=== Stock vs Sales vs Orders (Top rows) ===")
    print(f"{'Distributor':15} {'SKU':25} {'Stock':>8} {'Sales':>8} {'Orders':>8}")
    print("-" * 70)

    count = 0
    for key, stock in stock_state.items():
        if count >= max_rows:
            break
        dist, sku = key
        sales = sales_state.get(key, 0)
        orders = order_state.get(key, 0)
        print(f"{dist:15} {sku:25} {stock:8d} {sales:8d} {orders:8d}")
        count += 1


def get_snapshot_rows(stock_state, sales_state, order_state):
    rows = []
    for (dist, sku), stock in stock_state.items():
        rows.append(
            {
                "distributor_id": dist,
                "sku_code": sku,
                "stock": stock,
                "sales": sales_state.get((dist, sku), 0),
                "orders": order_state.get((dist, sku), 0),
            }
        )
    return rows


def main():
    # 1) Consumers
    sales_consumer = create_consumer(SALES_TOPIC)
    inv_consumer = create_consumer(INV_TOPIC)
    order_consumer = create_consumer(ORDER_TOPIC)

    # 2) State
    stock_state = defaultdict(int)
    sales_state = defaultdict(int)
    order_state = defaultdict(int)

    # 3) Alert producer
    alert_producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting real-time Stock vs Sales vs Orders calculator...")

    while True:
        inv_records = inv_consumer.poll(timeout_ms=500)
        sales_records = sales_consumer.poll(timeout_ms=500)
        order_records = order_consumer.poll(timeout_ms=500)

        # 4) inventory.movements
        for _, messages in inv_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                stock_state[key] += qty

                print("[INV] ", key, "qty_change:", qty, "stock:", stock_state[key])

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

        # 5) sales.events
        for _, messages in sales_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
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

        # 6) order.events
        for _, messages in order_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                order_state[key] += qty

                print("[ORD] ", key, "order_qty+:", qty, "total_orders:", order_state[key])

        # 7) Dashboard + MySQL snapshot
        print_dashboard(stock_state, sales_state, order_state)
        snapshot_rows = get_snapshot_rows(stock_state, sales_state, order_state)
        upsert_snapshot_rows(snapshot_rows)


if __name__ == "__main__":
    main()


