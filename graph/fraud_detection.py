import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from build_graph import build_graph, create_spark
from config.config import DELTA_PATH

def detect_fraud(g):
    # --- PageRank : comptes qui reçoivent beaucoup de flux (hubs suspects) ---
    print("=== PageRank ===")
    pagerank = g.pageRank(resetProbability=0.15, maxIter=10)
    pagerank.vertices.orderBy("pagerank", ascending=False).show(10)

    # Sauvegarder les noeuds suspects (score élevé)
    suspicious_nodes = pagerank.vertices.filter("pagerank > 1.0")
    suspicious_nodes.write.format("delta").mode("overwrite") \
        .save(f"{DELTA_PATH}/suspicious_nodes")
    print(f"Noeuds suspects sauvegardés : {suspicious_nodes.count()}")

    # --- Label Propagation : communautés de comptes complices ---
    print("\n=== Label Propagation : communautés ===")
    communities = g.labelPropagation(maxIter=5)
    communities.groupBy("label").count() \
        .orderBy("count", ascending=False).show(10)

    return pagerank, communities

if __name__ == "__main__":
    spark = create_spark()
    g = build_graph(spark)
    detect_fraud(g)
