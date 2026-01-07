import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Thread, Lock

from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer, KafkaProducer

# ----------------------------
# CONFIG
# ----------------------------
BROKER = "localhost:9092"

SALES_TOPIC = "sales.events"
INV_TOPIC = "inventory.movements"
ORDER_TOPIC = "order.events"
ALERT_TOPIC = "alerts.lowstock"

LOW_STOCK_THRESHOLD = 50

# rolling windows
MAX_POINTS = 240           # 240 points * 5s = ~20 minutes
METRIC_INTERVAL_SEC = 5

SKU_CATEGORY = {
    "SKU_PRAN_JUICE_250ML": "Food & Beverage",
    "SKU_NESTLE_MILK_1L": "Food & Beverage",
    "SKU_AKIJ_SOFTDRINK_500ML": "Food & Beverage",
    "SKU_ACI_SALT_1KG": "Household Care",
}

# ----------------------------
# GLOBAL STATE (IN-MEMORY)
# ----------------------------
app = Flask(__name__)
lock = Lock()

stock_state = defaultdict(int)     # (dist, sku) -> stock
sales_state = defaultdict(int)     # (dist, sku) -> cum_sales_qty
order_state = defaultdict(int)     # (dist, sku) -> cum_orders_qty

initialized_pairs = set()          # (dist, sku) seen via inventory topic

last_alert_ts = {}                 # (dist, sku) -> epoch seconds (debounce)
recent_alerts = deque(maxlen=200)  # dict alert payloads

recent_sales = deque(maxlen=20000)      # (ts_epoch, dist, sku, qty)
recent_oos_events = deque(maxlen=20000) # (ts_epoch, dist, sku, stock_after)

ts_labels = deque(maxlen=MAX_POINTS)
series_turnover = deque(maxlen=MAX_POINTS)
series_oos_rate = deque(maxlen=MAX_POINTS)
series_otif = deque(maxlen=MAX_POINTS)
series_infull = deque(maxlen=MAX_POINTS)


# ----------------------------
# KAFKA HELPERS
# ----------------------------
def create_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=group_id,
    )


def _now_epoch():
    return time.time()


def _iso_now():
    return datetime.now(timezone.utc).isoformat()


def _category_for_sku(sku: str):
    return SKU_CATEGORY.get(sku, "Other")


# ----------------------------
# ALERTING
# ----------------------------
def _maybe_send_alert(dist, sku, current_stock, alert_producer):
    if current_stock >= LOW_STOCK_THRESHOLD:
        return

    key = (dist, sku)
    now = _now_epoch()

    # debounce: at most one alert per (dist,sku) per 30 seconds
    last = last_alert_ts.get(key, 0)
    if now - last < 30:
        return
    last_alert_ts[key] = now

    alert = {
        "event_type": "LOW_STOCK",
        "event_ts": _iso_now(),
        "distributor_id": dist,
        "sku_code": sku,
        "current_stock": current_stock,
        "threshold": LOW_STOCK_THRESHOLD,
        "source_system": "FLASK_DASHBOARD",
    }

    alert_producer.send(ALERT_TOPIC, alert)

    with lock:
        recent_alerts.appendleft(alert)


# ----------------------------
# KPI COMPUTATION (PROXIES)
# ----------------------------
def _compute_oos_rate_percent():
    # OOS rate = % of initialized (dist,sku) pairs where stock <= 0
    with lock:
        if not initialized_pairs:
            return 0.0
        total = len(initialized_pairs)
        oos = sum(1 for k in initialized_pairs if stock_state.get(k, 0) <= 0)
    return (oos / total) * 100.0


def _avg_stock_initialized_pairs():
    with lock:
        if not initialized_pairs:
            return 0.0
        return sum(stock_state.get(k, 0) for k in initialized_pairs) / max(len(initialized_pairs), 1)


def _sales_rate_per_minute(window_sec=15 * 60):
    cutoff = _now_epoch() - window_sec
    with lock:
        sales = [x for x in recent_sales if x[0] >= cutoff]
    total_qty = sum(qty for _, _, _, qty in sales)
    minutes = max(window_sec / 60.0, 1.0)
    return total_qty / minutes


def _inventory_turnover_proxy():
    avg_stock = _avg_stock_initialized_pairs()
    sales_rate = _sales_rate_per_minute()
    if avg_stock <= 0:
        return 0.0
    raw = sales_rate / avg_stock
    return max(0.0, raw * 100.0)


def _in_full_rate_proxy():
    # In-full proxy: % of pairs where current stock >= min(orders, 50)
    with lock:
        keys = set(order_state.keys())
        if not keys:
            return 0.0
        ok = 0
        for k in keys:
            ord_qty = order_state.get(k, 0)
            st = stock_state.get(k, 0)
            if ord_qty > 0 and st >= min(ord_qty, 50):
                ok += 1
        return (ok / len(keys)) * 100.0


def _otif_rate_proxy():
    infull = _in_full_rate_proxy()
    oos = _compute_oos_rate_percent()
    otif = infull * (1.0 - (oos / 100.0))
    return max(0.0, min(100.0, otif))


def _avg_days_to_sell_by_category():
    cutoff = _now_epoch() - (30 * 60)  # last 30 minutes
    with lock:
        sales = [x for x in recent_sales if x[0] >= cutoff]
        stock_snapshot = dict(stock_state)

    sales_by_sku = defaultdict(int)
    for _, _, sku, qty in sales:
        sales_by_sku[sku] += qty

    cat_stock = defaultdict(int)
    cat_sales = defaultdict(int)

    for (_, sku), st in stock_snapshot.items():
        cat = _category_for_sku(sku)
        cat_stock[cat] += st

    for sku, s in sales_by_sku.items():
        cat = _category_for_sku(sku)
        cat_sales[cat] += s

    result = {}
    cats = set(list(cat_stock.keys()) + list(cat_sales.keys()))
    for cat in cats:
        daily_sales = (cat_sales.get(cat, 0) * 48)  # 30min -> day estimate
        days = cat_stock.get(cat, 0) / (daily_sales + 1)
        result[cat] = round(days, 2)

    for fixed in ["Food & Beverage", "Household Care", "Personal Care", "Tobacco"]:
        result.setdefault(fixed, 0)

    return result


def _freshness_buckets():
    cutoff = _now_epoch() - (60 * 60)
    with lock:
        sales = [x for x in recent_sales if x[0] >= cutoff]
    total = sum(qty for _, _, _, qty in sales)

    buckets = {
        "< 3 Days": 0,
        "3-7 Days": 0,
        "8-14 Days": 0,
        "15-28 Days": 0,
        "> 28 Days": 0,
    }
    if total <= 0:
        return buckets

    buckets["< 3 Days"] = int(total * 0.25)
    buckets["3-7 Days"] = int(total * 0.30)
    buckets["8-14 Days"] = int(total * 0.25)
    buckets["15-28 Days"] = int(total * 0.18)
    buckets["> 28 Days"] = max(0, total - sum(buckets.values()))
    return buckets


def _oos_by_hour_last_24h():
    cutoff = _now_epoch() - (24 * 60 * 60)
    hours = {f"{h:02d}": 0 for h in range(24)}
    with lock:
        oos = [x for x in recent_oos_events if x[0] >= cutoff]
    for ts, _, _, _ in oos:
        h = datetime.fromtimestamp(ts).strftime("%H")
        hours[h] += 1
    return hours


def _oos_by_dow_last_7d():
    cutoff = _now_epoch() - (7 * 24 * 60 * 60)
    days = {"Mon": 0, "Tue": 0, "Wed": 0, "Thu": 0, "Fri": 0, "Sat": 0, "Sun": 0}
    with lock:
        oos = [x for x in recent_oos_events if x[0] >= cutoff]
    for ts, _, _, _ in oos:
        d = datetime.fromtimestamp(ts).strftime("%a")
        if d in days:
            days[d] += 1
    return days


def _stock_rows(limit=25):
    with lock:
        keys = set(stock_state.keys()) | set(sales_state.keys()) | set(order_state.keys())
        rows = []
        for (dist, sku) in keys:
            st = stock_state.get((dist, sku), 0)
            sl = sales_state.get((dist, sku), 0)
            od = order_state.get((dist, sku), 0)

            if st <= 0 and (dist, sku) in initialized_pairs:
                status = "OOS"
            elif st < LOW_STOCK_THRESHOLD and (dist, sku) in initialized_pairs:
                status = "LOW"
            else:
                status = "OK"

            rows.append(
                {
                    "distributor_id": dist,
                    "sku_code": sku,
                    "stock": st,
                    "sales": sl,
                    "orders": od,
                    "status": status,
                }
            )

        priority = {"OOS": 0, "LOW": 1, "OK": 2}
        rows.sort(key=lambda r: (priority[r["status"]], r["stock"]))
        return rows[:limit]


# ----------------------------
# BACKGROUND WORKERS
# ----------------------------
def kafka_state_updater():
    sales_consumer = create_consumer(SALES_TOPIC, "dash-sales-group")
    inv_consumer = create_consumer(INV_TOPIC, "dash-inv-group")
    order_consumer = create_consumer(ORDER_TOPIC, "dash-order-group")

    alert_producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        inv_records = inv_consumer.poll(timeout_ms=500)
        sales_records = sales_consumer.poll(timeout_ms=500)
        order_records = order_consumer.poll(timeout_ms=500)

        now_ts = _now_epoch()

        for _, messages in inv_records.items():
            for msg in messages:
                e = msg.value
                dist = e.get("distributor_id")
                sku = e.get("sku_code")
                qty = int(e.get("quantity", 0) or 0)
                if not dist or not sku:
                    continue

                with lock:
                    stock_state[(dist, sku)] += qty
                    initialized_pairs.add((dist, sku))
                    st = stock_state[(dist, sku)]

                _maybe_send_alert(dist, sku, st, alert_producer)
                if st <= 0:
                    with lock:
                        recent_oos_events.append((now_ts, dist, sku, st))

        for _, messages in sales_records.items():
            for msg in messages:
                e = msg.value
                dist = e.get("distributor_id")
                sku = e.get("sku_code")
                qty = int(e.get("quantity", 0) or 0)
                if not dist or not sku:
                    continue

                with lock:
                    stock_state[(dist, sku)] -= qty
                    sales_state[(dist, sku)] += qty
                    recent_sales.append((now_ts, dist, sku, qty))
                    st = stock_state[(dist, sku)]

                _maybe_send_alert(dist, sku, st, alert_producer)
                if st <= 0:
                    with lock:
                        recent_oos_events.append((now_ts, dist, sku, st))

        for _, messages in order_records.items():
            for msg in messages:
                e = msg.value
                dist = e.get("distributor_id")
                sku = e.get("sku_code")
                qty = int(e.get("quantity", 0) or 0)
                if not dist or not sku:
                    continue

                with lock:
                    order_state[(dist, sku)] += qty


def metric_sampler():
    while True:
        ts = datetime.now().strftime("%H:%M:%S")

        turnover = round(_inventory_turnover_proxy(), 2)
        oos = round(_compute_oos_rate_percent(), 2)
        infull = round(_in_full_rate_proxy(), 2)
        otif = round(_otif_rate_proxy(), 2)

        with lock:
            ts_labels.append(ts)
            series_turnover.append(turnover)
            series_oos_rate.append(oos)
            series_infull.append(infull)
            series_otif.append(otif)

        time.sleep(METRIC_INTERVAL_SEC)


# ----------------------------
# API ENDPOINTS
# ----------------------------
@app.route("/api/summary")
def api_summary():
    with lock:
        total_pairs = len(initialized_pairs)
        low_stock_pairs = sum(1 for k in initialized_pairs if stock_state.get(k, 0) < LOW_STOCK_THRESHOLD)
        oos_pairs = sum(1 for k in initialized_pairs if stock_state.get(k, 0) <= 0)

    payload = {
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kpi": {
            "inventory_turnover": round(_inventory_turnover_proxy(), 2),
            "oos_rate": round(_compute_oos_rate_percent(), 2),
            "in_full_rate": round(_in_full_rate_proxy(), 2),
            "otif_rate": round(_otif_rate_proxy(), 2),
            "low_stock_pairs": low_stock_pairs,
            "oos_pairs": oos_pairs,
            "total_pairs": total_pairs,
        },
        "avg_days_to_sell_by_category": _avg_days_to_sell_by_category(),
        "freshness_buckets": _freshness_buckets(),
        "oos_by_hour": _oos_by_hour_last_24h(),
        "oos_by_dow": _oos_by_dow_last_7d(),
        "recent_alerts": list(recent_alerts)[:20],
    }
    return jsonify(payload)


@app.route("/api/timeseries")
def api_timeseries():
    with lock:
        return jsonify(
            {
                "labels": list(ts_labels),
                "turnover": list(series_turnover),
                "oos": list(series_oos_rate),
                "infull": list(series_infull),
                "otif": list(series_otif),
            }
        )


@app.route("/api/stock_table")
def api_stock_table():
    return jsonify({"rows": _stock_rows(limit=25)})


# ----------------------------
# DASHBOARD UI
# ----------------------------
DASHBOARD_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>FMCG Live KPI Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root{
      --bg:#0b1220;
      --card:#121b2e;
      --muted:#98a2b3;
      --text:#e5e7eb;
      --accent:#22c55e;
      --warn:#f59e0b;
      --danger:#ef4444;
      --blue:#3b82f6;
      --cyan:#06b6d4;
      --violet:#8b5cf6;
      --border: rgba(255,255,255,.08);
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: Arial, sans-serif;
      background:
        radial-gradient(1200px 600px at 20% -10%, rgba(34,197,94,.25), transparent 60%),
        radial-gradient(800px 600px at 110% 10%, rgba(59,130,246,.18), transparent 60%),
        var(--bg);
      color:var(--text);
      overflow-x:hidden; /* prevent accidental horizontal scroll */
    }

    .container{
      width: min(1400px, 100%);
      margin: 0 auto;
      padding: 0 12px;
    }

    header{
      padding:18px 0 8px 0;
      display:flex;
      align-items:flex-end;
      justify-content:space-between;
      gap:12px;
      flex-wrap: wrap;
    }
    header h1{
      margin:0;
      font-size:clamp(16px, 2.2vw, 20px);
      letter-spacing:.2px;
      font-weight:700;
    }
    header .sub{
      color:var(--muted);
      font-size:12px;
      margin-top:6px;
      max-width: 70ch;
      line-height: 1.35;
    }
    .pill{
      border:1px solid var(--border);
      background: rgba(255,255,255,.03);
      padding:8px 10px;
      border-radius:10px;
      font-size:12px;
      color:var(--muted);
      display:flex;
      gap:10px;
      align-items:center;
      white-space:nowrap;
    }

    .grid{
      padding:12px 0 18px 0;
      display:grid;
      gap:12px;
      grid-template-areas:
        "left"
        "center"
        "right";
      grid-template-columns: 1fr;
    }
    @media (min-width: 980px){
      .grid{
        grid-template-areas:
          "left center"
          "right right";
        grid-template-columns: 340px 1fr;
      }
    }
    @media (min-width: 1400px){
      .grid{
        grid-template-areas: "left center right";
        grid-template-columns: 340px 1fr 420px;
        align-items: start;
      }
    }

    .leftPanel{ grid-area:left; }
    .centerPanel{ grid-area:center; }
    .rightPanel{ grid-area:right; }

    .card{
      background: linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.02));
      border:1px solid var(--border);
      border-radius:14px;
      padding:12px;
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
      min-width: 0; /* critical to prevent overflow inside grid */
    }
    .card h2{
      margin:0 0 10px 0;
      font-size:13px;
      color:var(--muted);
      font-weight:700;
      letter-spacing:.35px;
      text-transform:uppercase;
    }

    .gauges{ display:grid; gap:12px; }
    .gaugeRow{
      display:grid;
      grid-template-columns: 110px 1fr;
      gap:10px;
      align-items:center;
      min-width: 0;
    }
    .bigNumber{
      font-size:clamp(20px, 2.6vw, 28px);
      font-weight:800;
      line-height:1;
    }
    .smallMeta{
      color:var(--muted);
      font-size:12px;
      margin-top:6px;
      line-height: 1.35;
    }

    /* Chart wrappers: fixed height prevents Chart.js from stretching weirdly */
    .chartBox{ height: clamp(180px, 24vw, 220px); }
    .chartBoxTall{ height: clamp(220px, 30vw, 280px); }
    canvas{ width:100% !important; height:100% !important; }

    .kpiMini{
      display:grid;
      grid-template-columns: repeat(3, 1fr);
      gap:10px;
      margin-top:8px;
    }
    @media (max-width: 420px){
      .kpiMini{ grid-template-columns: 1fr; }
      .gaugeRow{ grid-template-columns: 90px 1fr; }
    }

    .miniCard{
      background: rgba(255,255,255,.03);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      min-width: 0;
    }
    .miniVal{font-size:18px; font-weight:800;}
    .miniLbl{color:var(--muted); font-size:11px; margin-top:4px;}

    /* Tables */
    .tableOuter{
      width:100%;
      overflow-x:auto;     /* horizontal scroll on small screens */
      -webkit-overflow-scrolling: touch;
      border-radius: 10px;
    }
    table.table{
      width:100%;
      border-collapse:collapse;
      font-size:12px;
      color:var(--text);
      min-width: 520px; /* allow scroll instead of breaking layout */
    }
    .table th, .table td{
      padding:8px 8px;
      border-bottom:1px solid rgba(255,255,255,.06);
      text-align:left;
      vertical-align:top;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 220px;
    }
    .table td:nth-child(2){ max-width: 260px; } /* SKU column */
    .tag{
      display:inline-block;
      padding:2px 8px;
      border-radius:999px;
      font-size:11px;
      border:1px solid var(--border);
      color:var(--muted);
      background: rgba(255,255,255,.02);
    }
    .tag.danger{border-color: rgba(239,68,68,.4); color:#fecaca; background: rgba(239,68,68,.12);}
    .tag.warn{border-color: rgba(245,158,11,.4); color:#fde68a; background: rgba(245,158,11,.10);}

    .rightStack{display:grid; gap:12px;}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>FMCG Digital Transformation — Live KPI Dashboard</h1>
        <div class="sub">Kafka → Real-time Stock/Sales/Orders → Alerts → Flask Live Dashboard</div>
      </div>
      <div class="pill">
        <span>Last update:</span>
        <strong id="lastUpdate">--</strong>
        <span class="tag" id="pipelineState">RUNNING</span>
      </div>
    </header>

    <div class="grid">
      <!-- LEFT -->
      <div class="card gauges leftPanel">
        <h2>Core KPIs</h2>

        <div class="gaugeRow">
          <div class="chartBox"><canvas id="g_turnover"></canvas></div>
          <div>
            <div class="bigNumber"><span id="k_turnover">0</span></div>
            <div class="smallMeta">Inventory turnover (proxy)</div>
          </div>
        </div>

        <div class="gaugeRow">
          <div class="chartBox"><canvas id="g_oos"></canvas></div>
          <div>
            <div class="bigNumber"><span id="k_oos">0</span>%</div>
            <div class="smallMeta">Out-of-stock rate (initialized pairs)</div>
          </div>
        </div>

        <div class="gaugeRow">
          <div class="chartBox"><canvas id="g_infull"></canvas></div>
          <div>
            <div class="bigNumber"><span id="k_infull">0</span>%</div>
            <div class="smallMeta">In-Full rate (proxy)</div>
          </div>
        </div>

        <div class="gaugeRow">
          <div class="chartBox"><canvas id="g_otif"></canvas></div>
          <div>
            <div class="bigNumber"><span id="k_otif">0</span>%</div>
            <div class="smallMeta">OTIF rate (proxy)</div>
          </div>
        </div>

        <div class="kpiMini">
          <div class="miniCard">
            <div class="miniVal" id="k_pairs">0</div>
            <div class="miniLbl">Pairs tracked</div>
          </div>
          <div class="miniCard">
            <div class="miniVal" id="k_low">0</div>
            <div class="miniLbl">Low-stock pairs</div>
          </div>
          <div class="miniCard">
            <div class="miniVal" id="k_oos_pairs">0</div>
            <div class="miniLbl">OOS pairs</div>
          </div>
        </div>
      </div>

      <!-- CENTER -->
      <div class="card centerPanel">
        <h2>Operational Trends</h2>
        <div class="chartBoxTall"><canvas id="c_timeseries"></canvas></div>
        <div style="display:grid; grid-template-columns:1fr; gap:12px; margin-top:12px;">
          <div class="card" style="padding:10px; min-width:0;">
            <h2>OOS by day of week</h2>
            <div class="chartBox"><canvas id="c_oos_dow"></canvas></div>
          </div>
          <div class="card" style="padding:10px; min-width:0;">
            <h2>OOS by hour (24h)</h2>
            <div class="chartBox"><canvas id="c_oos_hour"></canvas></div>
          </div>
        </div>
      </div>

      <!-- RIGHT -->
      <div class="rightStack rightPanel">

        <div class="card">
          <h2>Live Stock vs Sales vs Orders</h2>
          <div class="tableOuter">
            <table class="table">
              <thead>
                <tr>
                  <th>Distributor</th><th>SKU</th><th>Stock</th><th>Sales</th><th>Orders</th><th>Status</th>
                </tr>
              </thead>
              <tbody id="stockBody"></tbody>
            </table>
          </div>
          <div class="smallMeta">Sorted: OOS → LOW → OK (lowest stock first). Scroll horizontally on mobile.</div>
        </div>

        <div class="card">
          <h2>Average time to sell (days)</h2>
          <div class="chartBox"><canvas id="c_days_to_sell"></canvas></div>
          <div class="smallMeta">Proxy based on stock vs recent sales velocity.</div>
        </div>

        <div class="card">
          <h2>Sold within freshness date</h2>
          <div class="chartBox"><canvas id="c_freshness"></canvas></div>
        </div>

        <div class="card">
          <h2>Recent low-stock alerts</h2>
          <div class="tableOuter">
            <table class="table">
              <thead>
                <tr>
                  <th>Time</th><th>Distributor</th><th>SKU</th><th>Stock</th>
                </tr>
              </thead>
              <tbody id="alertsBody"></tbody>
            </table>
          </div>
        </div>

      </div>
    </div>
  </div>

<script>
  // --------- Chart.js gauge needle ----------
  const gaugeNeedlePlugin = {
    id: 'gaugeNeedle',
    afterDatasetDraw(chart) {
      const needleValue = chart?.config?.options?.needleValue ?? 0;
      const { ctx } = chart;
      const meta = chart.getDatasetMeta(0);
      if (!meta?.data?.length) return;
      const xCenter = meta.data[0].x;
      const yCenter = meta.data[0].y;
      const outerRadius = meta.data[0].outerRadius;

      const angle = Math.PI * (-0.5 + (needleValue / 100.0));

      ctx.save();
      ctx.translate(xCenter, yCenter);
      ctx.rotate(angle);

      ctx.beginPath();
      ctx.moveTo(0, -2);
      ctx.lineTo(outerRadius - 8, 0);
      ctx.lineTo(0, 2);
      ctx.fillStyle = 'rgba(229,231,235,.9)';
      ctx.fill();

      ctx.rotate(-angle);
      ctx.beginPath();
      ctx.arc(0, 0, 4, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(229,231,235,.9)';
      ctx.fill();
      ctx.restore();
    }
  };

  function makeGauge(ctx, color){
    return new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['value','rest'],
        datasets: [{
          data: [50, 50],
          backgroundColor: [color, 'rgba(255,255,255,.08)'],
          borderWidth: 0,
          cutout: '75%',
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display:false }, tooltip: { enabled:false } },
        rotation: -90,
        circumference: 180,
        needleValue: 50
      },
      plugins: [gaugeNeedlePlugin]
    });
  }

  function setGauge(gauge, value){
    const v = Math.max(0, Math.min(100, value));
    gauge.data.datasets[0].data = [v, 100-v];
    gauge.options.needleValue = v;
    gauge.update();
  }

  // --------- Charts init ----------
  const gTurnover = makeGauge(document.getElementById('g_turnover'), 'rgba(34,197,94,.85)');
  const gOOS      = makeGauge(document.getElementById('g_oos'),      'rgba(239,68,68,.85)');
  const gInFull   = makeGauge(document.getElementById('g_infull'),   'rgba(59,130,246,.85)');
  const gOTIF     = makeGauge(document.getElementById('g_otif'),     'rgba(6,182,212,.85)');

  const cTime = new Chart(document.getElementById('c_timeseries'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label:'Turnover', data: [], borderColor:'rgba(34,197,94,.9)', backgroundColor:'rgba(34,197,94,.15)', tension:.25 },
      { label:'OOS %', data: [], borderColor:'rgba(239,68,68,.9)', backgroundColor:'rgba(239,68,68,.12)', tension:.25 },
      { label:'In-Full %', data: [], borderColor:'rgba(59,130,246,.9)', backgroundColor:'rgba(59,130,246,.12)', tension:.25 },
      { label:'OTIF %', data: [], borderColor:'rgba(6,182,212,.9)', backgroundColor:'rgba(6,182,212,.12)', tension:.25 },
    ]},
    options: {
      responsive:true,
      plugins:{ legend:{ labels:{ color:'rgba(229,231,235,.85)' } } },
      scales:{
        x:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} },
        y:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} }
      }
    }
  });

  const cOOSDow = new Chart(document.getElementById('c_oos_dow'), {
    type:'bar',
    data:{ labels:[], datasets:[{ label:'OOS events', data:[], backgroundColor:'rgba(139,92,246,.75)' }] },
    options:{
      responsive:true,
      plugins:{ legend:{ display:false }},
      scales:{
        x:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} },
        y:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} }
      }
    }
  });

  const cOOSHour = new Chart(document.getElementById('c_oos_hour'), {
    type:'line',
    data:{ labels:[], datasets:[{ label:'OOS events', data:[], borderColor:'rgba(245,158,11,.9)', backgroundColor:'rgba(245,158,11,.18)', tension:.25 }] },
    options:{
      responsive:true,
      plugins:{ legend:{ display:false }},
      scales:{
        x:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} },
        y:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} }
      }
    }
  });

  const cDaysToSell = new Chart(document.getElementById('c_days_to_sell'), {
    type:'bar',
    data:{ labels:[], datasets:[{ label:'Days', data:[], backgroundColor:'rgba(59,130,246,.75)' }] },
    options:{
      responsive:true,
      plugins:{ legend:{ display:false }},
      indexAxis:'y',
      scales:{
        x:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} },
        y:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} }
      }
    }
  });

  const cFreshness = new Chart(document.getElementById('c_freshness'), {
    type:'bar',
    data:{ labels:[], datasets:[{ label:'Sales qty', data:[], backgroundColor:'rgba(34,197,94,.75)' }] },
    options:{
      responsive:true,
      plugins:{ legend:{ display:false }},
      scales:{
        x:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} },
        y:{ ticks:{ color:'rgba(152,162,179,.9)' }, grid:{ color:'rgba(255,255,255,.06)'} }
      }
    }
  });

  async function refreshAll(){
    try{
      const [summaryRes, tsRes, tableRes] = await Promise.all([
        fetch('/api/summary'),
        fetch('/api/timeseries'),
        fetch('/api/stock_table')
      ]);

      const summary = await summaryRes.json();
      const ts = await tsRes.json();
      const tableData = await tableRes.json();

      document.getElementById('lastUpdate').textContent = summary.now;

      document.getElementById('k_turnover').textContent = summary.kpi.inventory_turnover.toFixed(2);
      document.getElementById('k_oos').textContent = summary.kpi.oos_rate.toFixed(2);
      document.getElementById('k_infull').textContent = summary.kpi.in_full_rate.toFixed(2);
      document.getElementById('k_otif').textContent = summary.kpi.otif_rate.toFixed(2);

      document.getElementById('k_pairs').textContent = summary.kpi.total_pairs;
      document.getElementById('k_low').textContent = summary.kpi.low_stock_pairs;
      document.getElementById('k_oos_pairs').textContent = summary.kpi.oos_pairs;

      setGauge(gTurnover, Math.min(100, summary.kpi.inventory_turnover));
      setGauge(gOOS, summary.kpi.oos_rate);
      setGauge(gInFull, summary.kpi.in_full_rate);
      setGauge(gOTIF, summary.kpi.otif_rate);

      // Timeseries
      cTime.data.labels = ts.labels;
      cTime.data.datasets[0].data = ts.turnover;
      cTime.data.datasets[1].data = ts.oos;
      cTime.data.datasets[2].data = ts.infull;
      cTime.data.datasets[3].data = ts.otif;
      cTime.update();

      // OOS by DOW
      const dow = summary.oos_by_dow;
      cOOSDow.data.labels = Object.keys(dow);
      cOOSDow.data.datasets[0].data = Object.values(dow);
      cOOSDow.update();

      // OOS by hour
      const hour = summary.oos_by_hour;
      cOOSHour.data.labels = Object.keys(hour);
      cOOSHour.data.datasets[0].data = Object.values(hour);
      cOOSHour.update();

      // Days to sell
      const dts = summary.avg_days_to_sell_by_category;
      cDaysToSell.data.labels = Object.keys(dts);
      cDaysToSell.data.datasets[0].data = Object.values(dts);
      cDaysToSell.update();

      // Freshness
      const fr = summary.freshness_buckets;
      cFreshness.data.labels = Object.keys(fr);
      cFreshness.data.datasets[0].data = Object.values(fr);
      cFreshness.update();

      // Alerts table
      const alertsBody = document.getElementById('alertsBody');
      alertsBody.innerHTML = '';
      (summary.recent_alerts || []).slice(0, 12).forEach(a => {
        const tr = document.createElement('tr');
        const stock = a.current_stock;
        const tag = stock <= 0 ? 'danger' : 'warn';
        tr.innerHTML = `
          <td>${(a.event_ts || '').replace('T',' ').slice(0,19)}</td>
          <td title="${a.distributor_id || ''}">${a.distributor_id || ''}</td>
          <td title="${a.sku_code || ''}">${a.sku_code || ''}</td>
          <td><span class="tag ${tag}">${stock}</span></td>
        `;
        alertsBody.appendChild(tr);
      });

      // Live stock table
      const stockBody = document.getElementById('stockBody');
      stockBody.innerHTML = '';
      (tableData.rows || []).forEach(r => {
        const tagClass = r.status === 'OOS' ? 'danger' : (r.status === 'LOW' ? 'warn' : '');
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td title="${r.distributor_id}">${r.distributor_id}</td>
          <td title="${r.sku_code}">${r.sku_code}</td>
          <td>${r.stock}</td>
          <td>${r.sales}</td>
          <td>${r.orders}</td>
          <td><span class="tag ${tagClass}">${r.status}</span></td>
        `;
        stockBody.appendChild(tr);
      });

      document.getElementById('pipelineState').textContent = 'RUNNING';
      document.getElementById('pipelineState').className = 'tag';

    }catch(e){
      document.getElementById('pipelineState').textContent = 'ERROR';
      document.getElementById('pipelineState').className = 'tag danger';
    }
  }

  refreshAll();
  setInterval(refreshAll, 3000);
</script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


def start_background_threads():
    Thread(target=kafka_state_updater, daemon=True).start()
    Thread(target=metric_sampler, daemon=True).start()


if __name__ == "__main__":
    start_background_threads()
    app.run(host="0.0.0.0", port=5000, debug=True)


