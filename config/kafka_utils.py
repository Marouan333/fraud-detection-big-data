from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config.config import KAFKA_BROKER, KAFKA_TOPIC


def ensure_topic(partitions=4):
    """Cree le topic s'il n'existe pas : producteur et consommateur peuvent demarrer dans n'importe quel ordre."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    try:
        admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=partitions, replication_factor=1)])
        print(f"Topic {KAFKA_TOPIC} cree ({partitions} partitions)", flush=True)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
