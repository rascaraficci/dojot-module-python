import requests
import base64
import json
from .Logger import Log
from .Config import config

LOGGER = Log().color_log()
class Auth:

    def get_management_token(self, tenant):

        userinfo = {
            "username": config.dojot["management_service"],
            "service": tenant
        }

        jwt = "{}.{}.{}".format(base64.b64encode("model".encode()).decode(),
                                base64.b64encode(json.dumps(
                                    userinfo).encode()).decode(),
                                base64.b64encode("signature".encode()).decode())

        return jwt

    def get_tenants(self):

        url = config.auth['host'] + "/admin/tenants"
        ret = requests.get(url, headers={'authorization': self.get_management_token(config.dojot['management_service'])})
        payload = ret.json()
        
        return payload['tenants']

auth = Auth()