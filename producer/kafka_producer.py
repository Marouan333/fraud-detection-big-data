import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import json
import time
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter
from config.config import KAFKA_BROKER, KAFKA_TOPIC, PAYSIM_PATH, PROMETHEUS_PORT_PRODUCER

# Métriques Prometheus
transactions_sent = Counter('producer_transactions_total', 'Transactions envoyées à Kafka')
fraud_sent        = Counter('producer_fraud_total', 'Transactions frauduleuses envoyées')

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_paysim(producer, delay=0):
    df = pd.read_csv(PAYSIM_PATH)
    print(f"{len(df)} transactions chargées depuis PaySim")

    for i, (_, row) in enumerate(df.iterrows()):
        message = {
            "step":      int(row["step"]),
            "type":      row["type"],
            "amount":    float(row["amount"]),
            "nameOrig":  row["nameOrig"],
            "nameDest":  row["nameDest"],
            "isFraud":   int(row["isFraud"])
        }
        producer.send(KAFKA_TOPIC, value=message)
        transactions_sent.inc()

        if row["isFraud"] == 1:
            fraud_sent.inc()

        if i % 10000 == 0:
            print(f"[{i:,} / {len(df):,}] envoyées | fraudes : {int(fraud_sent._value.get())}")

        if delay:
            time.sleep(delay)

    producer.flush()
    print("Stream PaySim terminé.")

if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT_PRODUCER)
    print(f"Métriques exposées sur le port {PROMETHEUS_PORT_PRODUCER}")
    producer = create_producer()
    stream_paysim(producer)
