import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col
from graphframes import GraphFrame
from build_graph import create_spark
from config.config import IBM_AML_TRANS, DELTA_PATH

def detect_cycles(spark):
    print("=== Strongly Connected Components : cycles de blanchiment ===")
    print("Chargement des transactions de blanchiment...")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import concat_ws

    df = spark.read.csv(IBM_AML_TRANS, header=True, inferSchema=True)
    df = df.withColumn("src", concat_ws("_", col("`From Bank`").cast("string"), col("Account2"))) \
           .withColumn("dst", concat_ws("_", col("`To Bank`").cast("string"),   col("Account4")))

    # Sous-graphe : uniquement les transactions de blanchiment connues
    fraud_edges = df.filter(col("Is Laundering") == 1).select(
        "src", "dst",
        col("Amount Paid").alias("amount"),
        col("Payment Currency").alias("currency"),
        col("Payment Format").alias("format"),
        col("Is Laundering").alias("is_laundering")
    )
    fraud_edges.cache()
    n_fraud = fraud_edges.count()
    print(f"Arêtes de blanchiment : {n_fraud}")

    fraud_vertices = fraud_edges.select(col("src").alias("id")) \
        .union(fraud_edges.select(col("dst").alias("id"))).distinct()
    fraud_vertices.cache()
    n_nodes = fraud_vertices.count()
    print(f"Noeuds impliqués dans le blanchiment : {n_nodes}")

    g_fraud = GraphFrame(fraud_vertices, fraud_edges)

    scc = g_fraud.stronglyConnectedComponents(maxIter=10)
    scc.cache()

    component_sizes = scc.groupBy("component").count()
    cycles = component_sizes.filter("count > 1").orderBy("count", ascending=False)
    cycles.cache()
    n_cycles = cycles.count()
    print(f"Cycles détectés : {n_cycles}")
    cycles.show(20)

    cycle_nodes = scc.join(cycles, "component").select("id", "component", "count")
    cycle_edges = fraud_edges.join(
        cycle_nodes.select(col("id").alias("src"), "component"), "src"
    )
    n_laundering = cycle_edges.count()
    print(f"Transactions de blanchiment dans les cycles : {n_laundering}")
    cycle_edges.show(10)

    cycle_nodes.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/cycle_nodes")
    cycle_edges.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/laundering_cycles")
    print("Résultats sauvegardés dans Delta Lake")

    return scc, cycles

if __name__ == "__main__":
    spark = create_spark()
    detect_cycles(spark)
