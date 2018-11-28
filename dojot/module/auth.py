"""
Authentication module
"""

import base64
import json
import requests
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

        :rtype: list
        :return: List of tenants
        """
        url = self.config.auth['url'] + "/admin/tenants"
        ret = requests.get(url, headers={'authorization': "Bearer " + self.get_management_token()})
        payload = ret.json()

        return payload['tenants']
