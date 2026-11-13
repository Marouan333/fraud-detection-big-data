import os
from pyspark.sql import SparkSession
from config.config import APP_NAME, SPARK_MASTER, GRAPHFRAMES_JAR

DELTA_JAR = "io.delta:delta-spark_2.12:3.0.0"
KAFKA_JAR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def create_spark(app_suffix="", extra_packages=()):
    # En mode local, seule la memoire du driver compte (executor = driver)
    packages = ",".join((DELTA_JAR, *extra_packages))
    spark = SparkSession.builder \
        .appName(APP_NAME + app_suffix) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g")) \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.cleaner.periodicGC.interval", "1min") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_graph_spark(app_suffix=""):
    spark = create_spark(app_suffix, (GRAPHFRAMES_JAR,))
    # Requis par les algorithmes iteratifs de GraphFrames (SCC, composantes)
    spark.sparkContext.setCheckpointDir(os.path.join(os.environ.get("TEMP", "/tmp"), "spark_graph_ckpt"))
    return spark
