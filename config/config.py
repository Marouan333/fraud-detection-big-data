import os
import sys

# Force PySpark to use its own bundled JARs instead of any system-level Spark installation
# (SPARK_HOME pointing to a different Spark version causes Py4J constructor errors)
_pyspark_home = os.path.join(os.path.dirname(sys.executable), "..", "lib", "site-packages", "pyspark")
if not os.path.isdir(_pyspark_home):
    import pyspark
    _pyspark_home = os.path.dirname(pyspark.__file__)
os.environ["SPARK_HOME"] = _pyspark_home

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Chemins des datasets
DATA_DIR     = os.path.join(BASE_DIR, "data")
PAYSIM_PATH  = os.path.join(DATA_DIR, "paysim", "PS_20174392719_1491204439457_log.csv")
IBM_AML_TRANS     = os.path.join(DATA_DIR, "ibm_aml", "HI-Small_Trans.csv")
IBM_AML_ACCOUNTS  = os.path.join(DATA_DIR, "ibm_aml", "HI-Small_accounts.csv")

# Delta Lake
DELTA_PATH = os.path.join(BASE_DIR, "delta_lake")

# Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "transactions"

# Spark
APP_NAME     = "FraudDetection"
SPARK_MASTER = "local[*]"
GRAPHFRAMES_JAR = "graphframes:graphframes:0.8.3-spark3.5-s_2.12"

# Prometheus
PROMETHEUS_PORT_PRODUCER = 8000
PROMETHEUS_PORT_CONSUMER = 8001
