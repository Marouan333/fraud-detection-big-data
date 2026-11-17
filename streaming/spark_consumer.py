import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark import StorageLevel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DoubleType
from prometheus_client import start_http_server, Counter
from config.config import KAFKA_BROKER, KAFKA_TOPIC, DELTA_PATH, PROMETHEUS_PORT_CONSUMER
from config.spark_session import create_spark, KAFKA_JAR
from config.kafka_utils import ensure_topic

batches_processed      = Counter('consumer_batches_total', 'Batches traités')
transactions_processed = Counter('consumer_transactions_total', 'Transactions ingérées')
fraud_detected         = Counter('consumer_fraud_detected_total', 'Fraudes détectées en streaming')

SCHEMA = StructType([
    StructField("tx_id",    LongType()),
    StructField("step",     IntegerType()),
    StructField("type",     StringType()),
    StructField("amount",   DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("nameDest", StringType()),
    StructField("isFraud",  IntegerType()),
])

# Le checkpoint memorise les offsets Kafka deja traites : sans lui, chaque
# redemarrage relit le topic depuis le debut et duplique les donnees
CHECKPOINT = os.path.join(DELTA_PATH, "_checkpoints", "transactions")
APP_ID     = "paysim_stream"


def write_delta(df, table, batch_id):
    # txnAppId + txnVersion : si Spark rejoue un batch apres un echec, Delta ignore
    # l'ecriture deja commitee (exactly-once cote sink)
    df.write.format("delta").mode("append") \
        .option("txnAppId", f"{APP_ID}_{table}").option("txnVersion", batch_id) \
        .option("mergeSchema", "true") \
        .save(f"{DELTA_PATH}/{table}")


def process_batch(df, batch_id):
    # Le batch est reutilise 4 fois (2 comptages, 2 ecritures) : on le met en cache
    # pour ne relire Kafka et ne parser le JSON qu'une seule fois
    df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        frauds = df.filter(col("isFraud") == 1)
        count, fraud_count = df.count(), frauds.count()

        write_delta(df, "transactions", batch_id)
        if fraud_count:
            write_delta(frauds, "fraud_alerts", batch_id)

        transactions_processed.inc(count)
        fraud_detected.inc(fraud_count)
        batches_processed.inc()
        print(f"Batch {batch_id} | {count:,} transactions | {fraud_count} fraudes", flush=True)
    finally:
        df.unpersist()


def start_consumer(spark):
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200_000) \
        .option("minPartitions", 8) \
        .option("failOnDataLoss", "false") \
        .load()

    df = stream.select(from_json(col("value").cast("string"), SCHEMA).alias("data")).select("data.*")

    df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="10 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    ensure_topic()
    start_http_server(PROMETHEUS_PORT_CONSUMER)
    print(f"Métriques exposées sur le port {PROMETHEUS_PORT_CONSUMER}", flush=True)
    start_consumer(create_spark("_stream", (KAFKA_JAR,)))
