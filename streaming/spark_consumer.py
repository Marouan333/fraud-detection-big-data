import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from prometheus_client import start_http_server, Counter
from config.config import (KAFKA_BROKER, KAFKA_TOPIC, DELTA_PATH,
                           APP_NAME, SPARK_MASTER, PROMETHEUS_PORT_CONSUMER)

# Métriques Prometheus
batches_processed      = Counter('consumer_batches_total', 'Batches traités')
transactions_processed = Counter('consumer_transactions_total', 'Transactions ingérées')
fraud_detected         = Counter('consumer_fraud_detected_total', 'Fraudes détectées en streaming')

SCHEMA = StructType([
    StructField("step",     IntegerType()),
    StructField("type",     StringType()),
    StructField("amount",   DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("nameDest", StringType()),
    StructField("isFraud",  IntegerType())
])

def create_spark():
    return SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def process_batch(df, batch_id):
    count = df.count()
    fraud_count = df.filter(col("isFraud") == 1).count()

    transactions_processed.inc(count)
    batches_processed.inc()
    fraud_detected.inc(fraud_count)

    print(f"Batch {batch_id} | {count} transactions | {fraud_count} fraudes")

    df.write.format("delta").mode("append").save(f"{DELTA_PATH}/transactions")

    if fraud_count > 0:
        df.filter(col("isFraud") == 1) \
          .write.format("delta").mode("append").save(f"{DELTA_PATH}/fraud_alerts")

def start_consumer(spark):
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df = stream.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*")

    query = df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT_CONSUMER)
    print(f"Métriques exposées sur le port {PROMETHEUS_PORT_CONSUMER}")
    spark = create_spark()
    start_consumer(spark)
