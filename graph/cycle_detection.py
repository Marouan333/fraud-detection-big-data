import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col
from graphframes import GraphFrame
from build_graph import create_spark, load_edges, vertices_of
from config.config import DELTA_PATH


def detect_cycles(spark):
    print("=== Strongly Connected Components : cycles de blanchiment ===")

    # SCC sur le seul sous-graphe de blanchiment : sur les 5 M d'aretes le driver
    # sature (plus de 140 000 etapes Spark, OutOfMemoryError)
    fraud_edges = load_edges(spark).filter(col("is_laundering") == 1).cache()
    fraud_vertices = vertices_of(fraud_edges).cache()
    print(f"Aretes de blanchiment : {fraud_edges.count():,}")
    print(f"Noeuds impliques      : {fraud_vertices.count():,}")

    scc = GraphFrame(fraud_vertices, fraud_edges).stronglyConnectedComponents(maxIter=10).cache()

    cycles = scc.groupBy("component").count().filter("count > 1").cache()
    print(f"Cycles detectes : {cycles.count()}")
    cycles.orderBy("count", ascending=False).show(20)

    cycle_nodes = scc.join(cycles, "component").select("id", "component", "count")
    cycle_edges = fraud_edges.join(
        cycle_nodes.select(col("id").alias("src"), "component"), "src"
    )
    print(f"Transactions de blanchiment dans les cycles : {cycle_edges.count():,}")
    cycle_edges.show(10)

    cycle_nodes.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{DELTA_PATH}/cycle_nodes")
    cycle_edges.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{DELTA_PATH}/laundering_cycles")
    print("Resultats sauvegardes dans Delta Lake")
    return scc, cycles


if __name__ == "__main__":
    detect_cycles(create_spark("_cycles"))
