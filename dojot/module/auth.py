"""
Authentication module
"""

import base64
import json
import requests
import time
from .logger import Log
from .http_requester import HttpRequester


LOGGER = Log().color_log()
class Auth:
    """
    Class responsible for authentication mechanisms in dojot
    """
    def __init__(self, config):
        """
        Object initialization

        :type config: Config
        :param config: The configuration object.
        """
        self.config = config

    def get_management_token(self):
        """
        Retrieves a token for management operations

        :rtype: str
        :return: The token
        """

        userinfo = {
            "username": self.config.dojot["management"]["user"],
            "service": self.config.dojot["management"]["tenant"]
        }

        jwt = "{}.{}.{}".format(base64.b64encode("model".encode()).decode(),
                                base64.b64encode(json.dumps(
                                    userinfo).encode()).decode(),
                                base64.b64encode("signature".encode()).decode())

        return jwt

    def get_access_token(self, tenant):
        """
        Retrieves a token for normal operations associated to a particular
        tenant.

        :rtype: str
        :return: The token
        """

        userinfo = {
            "username": self.config.dojot["management"]["user"],
            "service": tenant
        }

        jwt = "{}.{}.{}".format(base64.b64encode("model".encode()).decode(),
                                base64.b64encode(json.dumps(
                                    userinfo).encode()).decode(),
                                base64.b64encode("signature".encode()).decode())

        return jwt


    def get_tenants(self):
        """
        Retrieves all tenants

        If there is a problem while retrieving the list of tenants, then None
        is returned.

        :rtype: list or None
        :return: List of tenants
        """

        url = self.config.auth['url'] + "/admin/tenants"
        retry_counter = self.config.auth["connection_retries"]
        timeout_sleep = self.config.auth["timeout_sleep"]
        payload = HttpRequester.do_it(url, self.get_management_token(), retry_counter, timeout_sleep)
        if payload is None:
            return None # because Python, that's because.
        return payload['tenants']
