import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import argparse
import json
import time
import pandas as pd
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter
from config.config import KAFKA_BROKER, KAFKA_TOPIC, PAYSIM_PATH, PROMETHEUS_PORT_PRODUCER

transactions_sent = Counter('producer_transactions_total', 'Transactions envoyées à Kafka')
fraud_sent        = Counter('producer_fraud_total', 'Transactions frauduleuses envoyées')
send_errors       = Counter('producer_errors_total', "Échecs d'envoi Kafka")

COLUMNS = ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]
DTYPES  = {"step": "int32", "type": "category", "amount": "float64",
           "nameOrig": "string", "nameDest": "string", "isFraud": "int8"}
CHUNK   = 100_000


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,
        linger_ms=20,              # regroupe les messages en lots au lieu d'un envoi par ligne
        batch_size=256 * 1024,
        compression_type='gzip',
    )


def on_error(exc):
    send_errors.inc()
    print(f"Erreur d'envoi Kafka : {exc}")


def stream_paysim(producer, delay=0.0, limit=None):
    sent = frauds = 0
    start = time.time()
    # Lecture par blocs : memoire bornee, seules les colonnes utiles sont chargees
    for chunk in pd.read_csv(PAYSIM_PATH, usecols=COLUMNS, dtype=DTYPES, chunksize=CHUNK):
        for row in chunk.itertuples(index=False):
            message = {
                "tx_id":    sent,      # identifiant stable : permet de dedoublonner un rejeu
                "step":     int(row.step),
                "type":     str(row.type),
                "amount":   float(row.amount),
                "nameOrig": row.nameOrig,
                "nameDest": row.nameDest,
                "isFraud":  int(row.isFraud),
            }
            # cle = compte emetteur : les transactions d'un compte restent ordonnees dans une partition
            producer.send(KAFKA_TOPIC, key=row.nameOrig, value=message).add_errback(on_error)
            sent += 1
            transactions_sent.inc()
            if message["isFraud"]:
                frauds += 1
                fraud_sent.inc()
            if delay:
                time.sleep(delay)
            if limit and sent >= limit:
                break

        rate = sent / (time.time() - start)
        print(f"[{sent:,}] envoyees | fraudes : {frauds:,} | {rate:,.0f} tx/s", flush=True)
        if limit and sent >= limit:
            break

    producer.flush()
    print(f"Stream PaySim termine : {sent:,} transactions en {time.time() - start:,.0f} s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Envoie PaySim dans Kafka")
    parser.add_argument("--delay", type=float, default=0.0, help="pause entre deux messages (s)")
    parser.add_argument("--limit", type=int, default=None, help="nombre max de transactions")
    args = parser.parse_args()

    start_http_server(PROMETHEUS_PORT_PRODUCER)
    print(f"Métriques exposées sur le port {PROMETHEUS_PORT_PRODUCER}")
    stream_paysim(create_producer(), args.delay, args.limit)
