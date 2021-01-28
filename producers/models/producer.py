"""Producer base-class providing common utilites and functionality"""
import logging
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092", # figure out the host
            "schema.registry.url": "http://localhost:8081"
            # "client.id": "ex4",
            # "linger.ms": 1000,
            # "compression.type": "lz4",
            # "batch.num.messages": 100,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties, default_key_schema=key_schema, default_value_schema=value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
        create_topic_task = client.create_topics([NewTopic(self.topic_name, 1, 1,
                config={
                    "cleanup.policy": "delete",
                    # "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                } )])
        for topic, t in create_topic_task.items():
            try:
                t.result()
                print("Topic {} created".format(topic))
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # Write cleanup code for the Producer here
        #
        #
        # logger.info("producer close incomplete - skipping")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
