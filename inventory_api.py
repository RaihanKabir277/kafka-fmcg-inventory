import json
from collections import defaultdict
from datetime import datetime, timezone
from threading import Thread

from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
SALES_TOPIC = "sales.events"
INV_TOPIC = "inventory.movements"
ORDER_TOPIC = "order.events"
ALERT_TOPIC = "alerts.lowstock"

LOW_STOCK_THRESHOLD = 50

app = Flask(__name__)

stock_state = defaultdict(int)
sales_state = defaultdict(int)
order_state = defaultdict(int)
alert_state = []  # keep recent alerts in memory


def create_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=group_id,
    )


def update_state():
    sales_consumer = create_consumer(SALES_TOPIC, "api-sales-group")
    inv_consumer = create_consumer(INV_TOPIC, "api-inv-group")
    order_consumer = create_consumer(ORDER_TOPIC, "api-order-group")

    alert_producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        inv_records = inv_consumer.poll(timeout_ms=500)
        sales_records = sales_consumer.poll(timeout_ms=500)
        order_records = order_consumer.poll(timeout_ms=500)

        # inventory movements
        for _, messages in inv_records.items():
            for msg in messages:
                e = msg.value
                key = (e.get("distributor_id"), e.get("sku_code"))
                qty = e.get("quantity", 0)
                stock_state[key] += qty

                _maybe_send_alert(
                    key, stock_state[key], alert_producer
                )

        # sales
        for _, messages in sales_records.items():
            for msg in messages:
                e = msg.value
                key = (e.get("distributor_id"), e.get("sku_code"))
                qty = e.get("quantity", 0)
                stock_state[key] -= qty
                sales_state[key] += qty

                _maybe_send_alert(
                    key, stock_state[key], alert_producer
                )

        # orders
        for _, messages in order_records.items():
            for msg in messages:
                e = msg.value
                key = (e.get("distributor_id"), e.get("sku_code"))
                order_state[key] += e.get("quantity", 0)


def _maybe_send_alert(key, current_stock, alert_producer):
    if current_stock >= LOW_STOCK_THRESHOLD:
        return

    distributor_id, sku_code = key
    alert = {
        "event_type": "LOW_STOCK",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "distributor_id": distributor_id,
        "sku_code": sku_code,
        "current_stock": current_stock,
        "threshold": LOW_STOCK_THRESHOLD,
        "source_system": "FLASK_API",
    }

    # send to Kafka
    alert_producer.send(ALERT_TOPIC, alert)

    # keep last 50 alerts in memory
    alert_state.append(alert)
    if len(alert_state) > 50:
        del alert_state[0]


@app.route("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.route("/inventory")
def inventory_summary():
    result = []
    for (dist, sku), stock in stock_state.items():
        result.append(
            {
                "distributor_id": dist,
                "sku_code": sku,
                "stock": stock,
                "sales": sales_state.get((dist, sku), 0),
                "orders": order_state.get((dist, sku), 0),
            }
        )
    return jsonify(result)


@app.route("/inventory/<distributor_id>/<sku_code>")
def inventory_detail(distributor_id, sku_code):
    key = (distributor_id, sku_code)
    data = {
        "distributor_id": distributor_id,
        "sku_code": sku_code,
        "stock": stock_state.get(key, 0),
        "sales": sales_state.get(key, 0),
        "orders": order_state.get(key, 0),
    }
    return jsonify(data)


@app.route("/alerts")
def alerts():
    return jsonify(alert_state)


# simple HTML dashboard that auto-refreshes
DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>FMCG Real-Time Inventory</title>
  <meta http-equiv="refresh" content="5"> <!-- refresh every 5 seconds -->
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { font-size: 22px; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
    th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }
    th { background-color: #f2f2f2; text-align: left; }
    tr.low-stock { background-color: #ffe5e5; }
  </style>
</head>
<body>
  <h1>FMCG Real-Time Stock vs Sales vs Orders</h1>
  <p>Last refresh: {{ now }}</p>

  <table>
    <tr>
      <th>Distributor</th>
      <th>SKU</th>
      <th>Stock</th>
      <th>Sales</th>
      <th>Orders</th>
    </tr>
    {% for (dist, sku), stock in stock_state.items() %}
      {% set sales = sales_state.get((dist, sku), 0) %}
      {% set orders = order_state.get((dist, sku), 0) %}
      <tr class="{{ 'low-stock' if stock < threshold else '' }}">
        <td>{{ dist }}</td>
        <td>{{ sku }}</td>
        <td>{{ stock }}</td>
        <td>{{ sales }}</td>
        <td>{{ orders }}</td>
      </tr>
    {% endfor %}
  </table>

  <h2>Recent Low-Stock Alerts</h2>
  <table>
    <tr>
      <th>Time</th>
      <th>Distributor</th>
      <th>SKU</th>
      <th>Current Stock</th>
      <th>Threshold</th>
    </tr>
    {% for a in alerts %}
      <tr>
        <td>{{ a["event_ts"] }}</td>
        <td>{{ a["distributor_id"] }}</td>
        <td>{{ a["sku_code"] }}</td>
        <td>{{ a["current_stock"] }}</td>
        <td>{{ a["threshold"] }}</td>
      </tr>
    {% endfor %}
  </table>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(
        DASHBOARD_HTML,
        stock_state=stock_state,
        sales_state=sales_state,
        order_state=order_state,
        alerts=alert_state,
        threshold=LOW_STOCK_THRESHOLD,
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def start_background_thread():
    t = Thread(target=update_state, daemon=True)
    t.start()


if __name__ == "__main__":
    start_background_thread()
    app.run(host="0.0.0.0", port=5000, debug=True)


