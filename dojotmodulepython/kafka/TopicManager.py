import requests
from ..Auth import auth

from ..Logger import Log

LOGGER = Log().color_log()
class TopicManager():

    def __init__(self):
        self.topics = dict()
        self.tenants = []

    def get_key(self, tenant, subject):
        return tenant + ":" + subject

    def get_topic(self, tenant, subject, broker, globalVal=""):
        
        key = self.get_key(tenant,subject)

        if(key in self.topics):
            return self.topics[key]
        else:
            if globalVal != "":
                querystring = "?global=true"
            else:
                querystring = ""

        url = broker + "/topic/" + subject + querystring
        ret = requests.get(url, headers={'authorization': "Bearer " + auth.get_management_token(tenant)})
        payload = ret.json()
        self.topics[key] = payload['topic']

        return self.topics[key]