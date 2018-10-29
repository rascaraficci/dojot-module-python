from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from ..Config import config
from .TopicManager import TopicManager
import json


class Producer:

    def __init__(self):
        self.broker = [config.kafka['host']]

    def init(self):
        try:
            self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          bootstrap_servers=self.broker)
            return 1
        except AssertionError as error:
            print("Ignoring assertion error on kafka producer %s" % error)
            return 0

    def produce(self, topic, msg):
        try:
            # send(topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None)
            self.producer.send(topic, msg)
            self.producer.flush()
        except KafkaTimeoutError:
            print("Kafka timed out")
