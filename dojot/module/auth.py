"""
Authentication module
"""

import base64
import json
import requests
import time
from .logger import Log

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
        payload = None
        while retry_counter > 0:
            try:
                LOGGER.debug("Retrieving list of tenants from auth...")
                ret = requests.get(
                    url, headers={'authorization': "Bearer " + self.get_management_token()})
                payload = ret.json()
                retry_counter = 0
                LOGGER.debug("... list of tenants retrieved from auth.")
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as connection_error:
                LOGGER.warning(
                    f"Connection error retrieving list of tenants: {connection_error}")
                LOGGER.warning(f"Sleeping for {self.config.auth['timeout_sleep']} before trying again.")
                time.sleep(self.config.auth["timeout_sleep"])
                LOGGER.warning("Retrying...")
                retry_counter -= 1
            except requests.exceptions.TooManyRedirects as redirects_error:
                # Nothing we can do about this.
                LOGGER.error(
                    f"Received too many redirects while retrieving list of tenants: {redirects_error}")
                retry_counter = 0

        if payload is None:
            LOGGER.warning("Tenant list request payload is still None.")
            return None

        return payload['tenants']
