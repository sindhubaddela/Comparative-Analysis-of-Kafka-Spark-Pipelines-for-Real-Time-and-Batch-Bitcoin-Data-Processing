import websocket

import json
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



# Updated on_message with validation
def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get('type') == 'ticker' and data.get('product_id') == 'BTC-USD':
            required_fields = [
                'price', 'best_bid', 'best_ask', 'side',
                'time', 'trade_id', 'last_size'
            ]
            if all(field in data for field in required_fields):
                print(f"[Producer] Publishing: {data}")
                producer.send('raw_prices', value=data)
            else:
                print("[Producer] Skipping incomplete message.")
        else:
            print("[Producer] Ignored non-ticker or unexpected product_id message.")
    except json.JSONDecodeError:
        print("[Producer] JSON decode error")
    except Exception as e:
        print(f"[Producer] Unexpected error: {e}")

# Coinbase subscription setup
def on_open(ws):
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
    ws.send(json.dumps(subscribe_msg))

# WebSocket connection setup
ws = websocket.WebSocketApp(
    "wss://ws-feed.exchange.coinbase.com",
    on_open=on_open,
    on_message=on_message
)

# Run the WebSocket
ws.run_forever()
