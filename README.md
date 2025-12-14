# Kafka-Based Real-Time Inventory & Sales Tracking (FMCG / Manufacturing)

Kafka + Python project for FMCG & Manufacturing (local market examples: PRAN, Nestlé, ACI, Akij) that streams Sales, Orders, and Inventory Movements into Kafka, computes real-time “Stock vs Sales vs Orders”, generates low-stock alerts, and exposes data via a Flask API + lightweight HTML dashboard.

## Project Structure   --> follow the below structure

kafka-fmcg-inventory/
infra/docker-compose.yml
producers/sales_producer/sales_producer.py
producers/order_producer/order_producer.py
producers/inventory_producer/inventory_producer.py
stream_processing/__init__.py
stream_processing/stock_calculator.py
stream_processing/db_writer.py
api/inventory_api.py
docs/SRS_KafkaProject_Retail.docx
.gitignore
README.md

## Requirements

Ubuntu / Linux
Docker + Docker Compose
Python 3.x + venv
(Optional) MySQL server

## Setup

git clone https://github.com/<your-username>/kafka-fmcg-inventory.git
cd kafka-fmcg-inventory
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install kafka-python Flask mysql-connector-python

## Start Kafka (Docker)

cd infra
docker compose up -d
docker compose ps
cd ..

## Create Kafka Topics

docker exec -it kafka bash
kafka-topics --create --topic sales.events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic order.events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic inventory.movements --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic alerts.lowstock --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
exit

## Run Producers (3 Terminals)

Terminal 1:
cd ~/Downloads/kafka-fmcg-inventory
source venv/bin/activate
python producers/sales_producer/sales_producer.py

Terminal 2:
cd ~/Downloads/kafka-fmcg-inventory
source venv/bin/activate
python producers/inventory_producer/inventory_producer.py

Terminal 3:
cd ~/Downloads/kafka-fmcg-inventory
source venv/bin/activate
python producers/order_producer/order_producer.py

## Run Stock Engine (Optional)

cd ~/Downloads/kafka-fmcg-inventory
source venv/bin/activate
touch stream_processing/__init__.py
python -m stream_processing.stock_calculator

## Run Flask API + Live Dashboard

cd ~/Downloads/kafka-fmcg-inventory
source venv/bin/activate
python api/inventory_api.py

Open:
http://localhost:5000/
http://localhost:5000/inventory
http://localhost:5000/alerts
http://localhost:5000/health

## View Kafka Data (Console)

Sales:
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic sales.events --from-beginning

Alerts:
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic alerts.lowstock --from-beginning

## Optional: MySQL (Snapshots for BI)

sudo apt update
sudo apt install -y mysql-server
sudo systemctl enable --now mysql

sudo mysql

CREATE DATABASE IF NOT EXISTS kafka_fmcg;
USE kafka_fmcg;

CREATE TABLE IF NOT EXISTS stock_snapshot (
  id INT AUTO_INCREMENT PRIMARY KEY,
  snapshot_ts DATETIME NOT NULL,
  distributor_id VARCHAR(50) NOT NULL,
  sku_code VARCHAR(100) NOT NULL,
  stock INT NOT NULL,
  sales INT NOT NULL,
  orders INT NOT NULL,
  UNIQUE KEY idx_dist_sku (distributor_id, sku_code)
);

CREATE USER IF NOT EXISTS 'bi_user'@'localhost' IDENTIFIED BY 'bi_password';
GRANT ALL PRIVILEGES ON kafka_fmcg.* TO 'bi_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;

mysql -u bi_user -p

USE kafka_fmcg;
SELECT * FROM stock_snapshot LIMIT 20;
EXIT;

## Stop / Clean

cd infra
docker compose down
cd ..

Stop running producers/services with Ctrl+C
deactivate

<!-- Kafka + Docker Compose reference: [web:165] -->
