"""
Topic manager
"""
import requests
from .. import Auth
from ..logger import Log
from ..http_requester import HttpRequester

LOGGER = Log().color_log()
class TopicManager():
    """
    Class that deals with DataBroker topics
    """

    def __init__(self, config):
        """
        TopicManager object initialization

        :type config: dojot.module.Config
        :param config: The configuration object
        """
        self.topics = dict()
        self.tenants = []
        self.broker = config.data_broker["url"]
        self.auth = Auth(config)
        self.retry_counter = config.data_broker["connection_retries"]
        self.timeout_sleep = config.data_broker["timeout_sleep"]

    @staticmethod
    def get_key(tenant, subject):
        """
        Return a key to be associated with a particular Kafka topic

        :type tenant: str
        :param tenant: The tenant to be used

        :type subject: str
        :param subject: The subject to be used

        :rtype: str
        :return: The key to be used
        """
        return tenant + ":" + subject

    def get_topic(self, tenant, subject, global_val=""):
        """
        Retrieves a topic from DataBroker

        :type tenant: str
        :param tenant: The tenant associated to the topic being retrieved

        :type subject: str
        :param subject: The subject associated to the topic being retrieved

        :type global_val: bool
        :param global_val: True if this topic should be a global one.

        :rtype: str
        :return: The topic
        """
        LOGGER.debug(f"Getting a topic for {subject}@{tenant}...")
        key = self.get_key(tenant, subject)

        if key in self.topics:
            LOGGER.debug(f"... got a cached entry.")
            LOGGER.debug(f"Entry is {self.topics[key]}.")
            return self.topics[key]

        if global_val != "":
            querystring = "?global=true"
        else:
            querystring = ""

        url = self.broker + "/topic/" + subject + querystring
        payload = HttpRequester.do_it(url, self.auth.get_access_token(tenant), self.retry_counter, self.timeout_sleep)

        if payload:
            self.topics[key] = payload['topic']
            return self.topics[key]
        else:
            return None
