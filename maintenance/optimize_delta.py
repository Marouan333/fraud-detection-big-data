"""Maintenance Delta Lake : dedoublonnage et compaction des tables de streaming.

Le streaming ecrit un petit fichier toutes les 10 s ; cette tache les regroupe et
retire les rejeux. Pas de VACUUM : l'historique reste consultable (time travel).
"""
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from delta.tables import DeltaTable
from config.config import DELTA_PATH
from config.spark_session import create_spark

# tx_id n'existe que pour les donnees produites apres son ajout : cle metier
KEY = ["step", "type", "amount", "nameOrig", "nameDest"]
TARGET_FILE_ROWS = 2_000_000


def dedupe_and_compact(spark, table):
    path = f"{DELTA_PATH}/{table}"
    df = spark.read.format("delta").load(path)
    before = df.count()
    clean = df.dropDuplicates(KEY).cache()
    after = clean.count()
    files = max(1, after // TARGET_FILE_ROWS)
    clean.repartition(files).write.format("delta").mode("overwrite").save(path)
    print(f"{table:<14} {before:>11,} -> {after:>11,} lignes | {files} fichier(s)")


def compact(spark, table):
    DeltaTable.forPath(spark, f"{DELTA_PATH}/{table}").optimize().executeCompaction()
    print(f"{table:<14} compacte")


if __name__ == "__main__":
    spark = create_spark("_maintenance")
    for t in ("transactions", "fraud_alerts"):
        dedupe_and_compact(spark, t)
    for t in ("suspicious_nodes", "cycle_nodes", "laundering_cycles"):
        compact(spark, t)
