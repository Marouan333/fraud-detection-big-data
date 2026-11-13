import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from graphframes import GraphFrame
from config.config import IBM_AML_TRANS
from config.spark_session import create_graph_spark as create_spark

# Schema explicite : evite la passe complete de inferSchema sur 5 M de lignes,
# et nomme les deux colonnes "Account" dupliquees dans l'en-tete du CSV
AML_SCHEMA = StructType([
    StructField("timestamp",          StringType()),
    StructField("from_bank",          StringType()),
    StructField("from_account",       StringType()),
    StructField("to_bank",            StringType()),
    StructField("to_account",         StringType()),
    StructField("amount_received",    DoubleType()),
    StructField("receiving_currency", StringType()),
    StructField("amount",             DoubleType()),
    StructField("currency",           StringType()),
    StructField("format",             StringType()),
    StructField("is_laundering",      IntegerType()),
])


def load_edges(spark):
    """Aretes du graphe : une ligne par transaction, comptes identifies par BankID_Account."""
    df = spark.read.csv(IBM_AML_TRANS, header=True, schema=AML_SCHEMA)
    # Un meme numero peut exister dans plusieurs banques : la banque fait partie de la cle.
    # cast int pour retirer les zeros de tete ("010" -> "10")
    return df.select(
        concat_ws("_", col("from_bank").cast("int").cast("string"), col("from_account")).alias("src"),
        concat_ws("_", col("to_bank").cast("int").cast("string"),   col("to_account")).alias("dst"),
        "amount", "currency", "format", "is_laundering",
    )


def vertices_of(edges):
    return edges.select(col("src").alias("id")) \
        .union(edges.select(col("dst").alias("id"))) \
        .distinct()


def build_graph(spark):
    edges = load_edges(spark).cache()
    g = GraphFrame(vertices_of(edges), edges)
    print(f"Graphe : {g.vertices.count():,} noeuds | {g.edges.count():,} aretes")
    return g


if __name__ == "__main__":
    build_graph(create_spark())
