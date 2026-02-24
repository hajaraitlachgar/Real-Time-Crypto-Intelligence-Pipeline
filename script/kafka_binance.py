import json
import websocket
from confluent_kafka import Producer
import time

conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'H42AXVVWCVXADIY4',
    'sasl.password': 'cfltyQtMG9goqwVI8oPnbJ0sdX37LmByy4waREnCnpfC4+qyQm2MiqCYZK61n6VA'
}
producer = Producer(conf)

SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "adausdt"]

stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]

    trade = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "event_time": int(time.time())
    }

    producer.produce("trades_topic", json.dumps(trade))
    producer.flush()

    print("Sent to Kafka:", trade)

def on_open(ws):
    print("Connected and streaming prices...")

ws = websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message
)

ws.run_forever()