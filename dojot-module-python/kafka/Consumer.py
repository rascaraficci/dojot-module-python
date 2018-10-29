from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError
from ..Config import config
from .TopicManager import TopicManager
import json
import time
import threading

class Consumer(threading.Thread):

    def __init__(self, group_id, name=None):
        threading.Thread.__init__(self)
        self.topics = []
        self.broker = [config.kafka['host']]
        self.group_id = group_id
        self.consumer = None
        print("Creating consumer on %s on group %s and topic %s" % (self.broker, self.group_id, self.topics))
        self.consumer = KafkaConsumer(bootstrap_servers=self.broker, group_id=self.group_id)
        print("Consumer created %s" % self.topics)

    def wait_init(self):
        init = False
        while not init:
            try:
                # self.consumer.poll()
                # self.consumer.seek_to_end()
                init = True
            except AssertionError as error:
                print("Ignoring assertion error %s %s" % (self.topics,error))
                time.sleep()

    def subscribe(self, topic, callback):
        try:
            print("Got a new topic to subscribe: %s" % topic)
            print("Current topic list: %s" % self.topics)
            self.topics.append(topic)
            self.consumer.subscribe(topics=self.topics)
            print(">>>>>>>>>><<<< >><Current subscriptions: %s" % (self.consumer.subscription()))
            print("Subscribed to topic %s" % topic)
            print("Current topic list: %s" % self.topics)
            self.callback = callback
        except Exception as error:
            print("Something went wrong while subscribing to topic %s: %s" % (topic, error))

    def run(self):
        print("[consumer] Now running consumer for topics: %s" % self.topics)
        self.wait_init()
        for msg in self.consumer:
            try:
                print("[run]msg topic: %s" % msg.topic)
                print("[run] msg value: %s" % msg.value)
                print("[run] callback: %s" % self.callback)

                self.callback(msg.topic, msg.value)
            except Exception as error:
                print("Data handler raised an unknown exception. Ignoring: %s" % error)

