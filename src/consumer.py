from kafka import KafkaConsumer
import sqlite3
import json

# Setup database connection
conn = sqlite3.connect("/app/data/crypto_prices.db")

cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS btc_prices (
    trade_id INTEGER PRIMARY KEY,
    price REAL,
    best_bid REAL,
    best_ask REAL,
    side TEXT,
    time TEXT,
    last_size REAL
)
""")
conn.commit()

# Kafka Consumer
consumer = KafkaConsumer(
    'raw_prices',
    bootstrap_servers='localhost:9092',
    group_id='btc_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Consumer] Waiting for messages...")
for msg in consumer:
    data = msg.value
    try:
        cursor.execute("""
        INSERT OR IGNORE INTO btc_prices
        (trade_id, price, best_bid, best_ask, side, time, last_size)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data['trade_id'], float(data['price']), float(data['best_bid']),
            float(data['best_ask']), data['side'], data['time'],
            float(data['last_size'])
        ))
        conn.commit()
        print(f"[Consumer] Saved trade_id {data['trade_id']}")
    except Exception as e:
        print(f"[Consumer] Error saving data: {e}")
