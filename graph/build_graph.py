import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from graphframes import GraphFrame
from config.config import IBM_AML_TRANS, APP_NAME, SPARK_MASTER, GRAPHFRAMES_JAR

def create_spark():
    packages = f"{GRAPHFRAMES_JAR},io.delta:delta-spark_2.12:3.0.0"
    return SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.cleaner.periodicGC.interval", "1min") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def build_graph(spark):
    df = spark.read.csv(IBM_AML_TRANS, header=True, inferSchema=True)

    # ID unique par compte : BankID_AccountNumber
    df = df.withColumn("src", concat_ws("_", col("`From Bank`").cast("string"), col("Account2"))) \
           .withColumn("dst", concat_ws("_", col("`To Bank`").cast("string"),   col("Account4")))

    # Noeuds = tous les comptes uniques
    vertices = df.select(col("src").alias("id")) \
                 .union(df.select(col("dst").alias("id"))) \
                 .distinct()

    # Arêtes = toutes les transactions
    edges = df.select(
        "src", "dst",
        col("Amount Paid").alias("amount"),
        col("Payment Currency").alias("currency"),
        col("Payment Format").alias("format"),
        col("Is Laundering").alias("is_laundering")
    )

    g = GraphFrame(vertices, edges)
    print(f"Graphe : {g.vertices.count()} noeuds | {g.edges.count()} arêtes")
    return g

if __name__ == "__main__":
    spark = create_spark()
    g = build_graph(spark)
