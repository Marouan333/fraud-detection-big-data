import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col, lit
from build_graph import build_graph, create_spark, vertices_of
from config.config import DELTA_PATH

TOP_PCT = 0.01

def laundering_accounts(g):
    return vertices_of(g.edges.filter(col("is_laundering") == 1))

def detect_fraud(g):
    # --- PageRank : comptes qui recoivent beaucoup de flux (hubs suspects) ---
    print("=== PageRank ===")
    pagerank = g.pageRank(resetProbability=0.15, maxIter=10)
    scores = pagerank.vertices.select("id", "pagerank").cache()
    n_total = scores.count()

    # Seuil = percentile 99 : la moyenne (1.0) retiendrait ~40 % des comptes
    threshold = scores.approxQuantile("pagerank", [1 - TOP_PCT], 0.0001)[0]
    print(f"Seuil PageRank (top {TOP_PCT:.0%}) : {threshold:.4f}")

    launder = laundering_accounts(g).withColumn("is_laundering_account", lit(1))
    suspicious = scores.filter(col("pagerank") >= threshold) \
        .join(launder, "id", "left").fillna(0, ["is_laundering_account"]).cache()

    n_susp   = suspicious.count()
    n_hits   = suspicious.filter("is_laundering_account = 1").count()
    n_laund  = launder.count()
    base     = n_laund / n_total
    precision = n_hits / n_susp
    print(f"Comptes suspects          : {n_susp:,} / {n_total:,}")
    print(f"Dont comptes blanchiment  : {n_hits:,} (precision {precision:.2%})")
    print(f"Taux de base (aleatoire)  : {base:.2%} -> lift x{precision / base:.1f}")
    print(f"Rappel                    : {n_hits / n_laund:.2%} des {n_laund:,} comptes de blanchiment")
    suspicious.orderBy("pagerank", ascending=False).show(10)

    suspicious.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{DELTA_PATH}/suspicious_nodes")
    print("Noeuds suspects sauvegardes dans Delta Lake")

    # --- Label Propagation : communautes de comptes complices ---
    print("\n=== Label Propagation : communautes ===")
    communities = g.labelPropagation(maxIter=5)
    communities.groupBy("label").count() \
        .orderBy("count", ascending=False).show(10)

    return pagerank, communities

if __name__ == "__main__":
    spark = create_spark("_pagerank")
    g = build_graph(spark)
    detect_fraud(g)
