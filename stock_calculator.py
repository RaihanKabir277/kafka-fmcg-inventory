import json
from collections import defaultdict

from kafka import KafkaConsumer

BROKER = "localhost:9092"
SALES_TOPIC = "sales.events"
INV_TOPIC = "inventory.movements"

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
    sales_consumer = create_consumer(SALES_TOPIC)
    inv_consumer = create_consumer(INV_TOPIC)

    stock_state = defaultdict(int)

    print("Starting real-time stock calculator...")
    while True:
        # poll both topics
        sales_records = sales_consumer.poll(timeout_ms=500)
        inv_records = inv_consumer.poll(timeout_ms=500)

        # process inventory movements
        for tp, messages in inv_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                stock_state[key] += qty

                print("[INV]", key, "qty_change:", qty, "stock:", stock_state[key])

        # process sales events
        for tp, messages in sales_records.items():
            for msg in messages:
                event = msg.value
                distributor_id = event.get("distributor_id")
                sku_code = event.get("sku_code")
                qty = event.get("quantity", 0)

                key = (distributor_id, sku_code)
                stock_state[key] -= qty

                print("[SALE]", key, "qty_change:", -qty, "stock:", stock_state[key])

if __name__ == "__main__":
    main()


