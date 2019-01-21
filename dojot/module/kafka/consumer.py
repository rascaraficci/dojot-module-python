"""
Kafka consumer module
"""

import threading
import time
from kafka import KafkaConsumer
from kafka.errors import IllegalStateError
from ..logger import Log

LOGGER = Log().color_log()

class Consumer(threading.Thread):
    """
    Kafka consumer
    """
    def __init__(self, config, name=None):
        """
        Builds a new Consumer

        It is important to realize that the `kafka.consumer` and
        `kafka.producer` configuration are directly passed to node-rdkafka
        library (which will forward it to librdkafka). You should check `its
        documentation<https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md>`_
        to know which are all the possible settings it offers.

        :type config: Config
        :param config: The configuration object
        :type name: str
        :param name: A name for this consumer.
        """
        threading.Thread.__init__(self)
        self.name = name
        self.topics = []
        self.poll_timeout = config.kafka['dojot']['poll_timeout']
        self.consumer = None
        self.callback = None
        LOGGER.info(f"Creating a new Kafka consumer {self.name}...");
        LOGGER.info(f"Configuration is:")
        LOGGER.info(f"{config.kafka['consumer']}")
        self.consumer = KafkaConsumer(**config.kafka["consumer"])
        LOGGER.info("... consumer created.")
        self.should_stop = threading.Event()
        self.should_reset_consumer = threading.Event()
        self.topic_lock = threading.RLock()
        self.subscription_holdoff = config.kafka["dojot"]["subscription_holdoff"]

    def subscribe(self, topic, callback):
        """
        Subscribe to a list of topics

        This function will add a new topic to the topics which this consumer
        is subscribed to.

        :type topic: str
        :param topic: new topic

        :type callback: function
        :param callback: Function to be called whenever a new message is received.
        This method should not thrown any exceptions. No processing is done
        with its results.
        """

        LOGGER.info("Got a new topic to subscribe: %s", topic)
        with self.topic_lock:
            self.topics.append(topic)

        self.callback = callback
        self.should_reset_consumer.set()

    def _reset_subscriptions(self):
        """
        Reset all subscriptions for this consumer.
        """
        try:
            LOGGER.info("Current topic list: %s", self.topics)
            self.consumer.subscribe(topics=self.topics)
            LOGGER.debug("Current subscriptions: %s", self.consumer.subscription())
        except IllegalStateError as error:
            LOGGER.critical("Kafka is in illegal state: %s", error)
        except AssertionError as error:
            LOGGER.critical("No topic was provided.")
        except TypeError as error:
            LOGGER.critical("Kafka listener type is invalid: %s", error)

    def stop(self):
        """
        Stop this consumer

        It won't be stopped right away, be advised.
        """
        self.should_stop.set()

    def run(self):
        LOGGER.debug(f"Now running consumer for topics: {self.topics}")
        while self.should_stop.is_set() is False:
            if self.should_reset_consumer.is_set():
                LOGGER.debug(f"Subscribing to a new list of topics...")
                local_topics = []
                while local_topics != self.topics:
                    with self.topic_lock:
                        local_topics = self.topics.copy()
                    LOGGER.debug(f"Holding off a bit before performing the subscription...")
                    time.sleep(self.subscription_holdoff)
                    LOGGER.debug("Ok, woke up just now.")
                    with self.topic_lock:
                        # If no new topics were added while this thread was
                        # sleeping
                        if local_topics == self.topics:
                            LOGGER.debug("... no further changes in topic list.")
                            LOGGER.debug("Creating subscriptions...")
                            self._reset_subscriptions()
                            LOGGER.debug("... subscriptions created")
                        else:
                            LOGGER.debug("... changes detected.")
                            LOGGER.debug("Will wait for more changes.")
                self.should_reset_consumer.clear()
                LOGGER.debug("... successfully subscribed to new list of topics.")

            topics = self.consumer.poll(self.poll_timeout)
            for topic in topics:
                for msg in topics[topic]:
                    self.callback(msg.topic, msg.value)
