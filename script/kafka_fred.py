import requests
import json
from confluent_kafka import Producer
from datetime import datetime

# =====================
# CONFIG
# =====================

FRED_API_KEY = "704fdac2c75a836581cb74008d766882"

conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'H42AXVVWCVXADIY4',
    'sasl.password': 'cfltyQtMG9goqwVI8oPnbJ0sdX37LmByy4waREnCnpfC4+qyQm2MiqCYZK61n6VA'
}

producer = Producer(conf)

SERIES_LIST = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE": "Unemployment Rate",
    "GDP": "Gross Domestic Product",
    "PCE": "Personal Consumption Expenditures",
    "M2SL": "M2 Money Stock",
    "PAYEMS": "Nonfarm Payrolls",
    "INDPRO": "Industrial Production",
    "FEDFUNDS": "Effective Fed Funds Rate",
    "TB3MS": "3-Month Treasury Bill Rate"
}

# =====================
# FETCH & SEND
# =====================

def fetch_latest(series_id, series_name):

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1
    }

    response = requests.get(url, params=params)
    data = response.json()

    observations = data.get("observations", [])
    if not observations:
        print("No data for", series_id)
        return

    latest = observations[0]

    event = {
        "indicator": series_name,
        "series_id": series_id,
        "date": latest["date"],
        "value": latest["value"],
        "fetched_at": datetime.utcnow().isoformat()
    }

    producer.produce("fred_topic", json.dumps(event))
    producer.poll(0)

    print("Sent:", event)


# =====================
# RUN ALL
# =====================

for series_id, name in SERIES_LIST.items():
    fetch_latest(series_id, name)

producer.flush()