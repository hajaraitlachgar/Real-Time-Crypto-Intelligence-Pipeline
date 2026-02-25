import requests
from confluent_kafka import Producer
import json
from datetime import datetime
import time

conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'H42AXVVWCVXADIY4',
    'sasl.password': 'cfltyQtMG9goqwVI8oPnbJ0sdX37LmByy4waREnCnpfC4+qyQm2MiqCYZK61n6VA'
}

producer = Producer(conf)

API_KEY = "b61e2b4006ce493599bb179be55cfc89"

def fetch_news():
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": '(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)',
        "sources": "reuters,bloomberg,cnbc,cointelegraph,financial-times",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data.get("articles", [])

while True:
    articles = fetch_news()
    for article in articles:
        news_event = {
            "symbol": "BTCUSDT", 
            "title": article['title'],
            "source": article['source']['name'],
            "publishedAt": article['publishedAt'],
            "fetchedAt": datetime.utcnow().isoformat()
        }
        producer.produce("news_topic", json.dumps(news_event))
        producer.flush()
        print("Sent news to Kafka:", news_event)
    time.sleep(60) 