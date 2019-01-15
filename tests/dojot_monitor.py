import falcon
import json
from dojot.module import Messenger, Config
from dojot.module.logger import Log
import base64
import threading

from wsgiref import simple_server

class DojotLogs:
    def __init__(self):
        self.messages = {}

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = f"{self.messages}"
        print(f"messages are {self.messages}")

class DojotSend:
    def __init__(self, messenger):
        self.messenger = messenger

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = "ok"
        messenger.publish("serviceStatus", "admin", "service X is up")


class DojotTopics:
    def on_get(self, req, resp, subject):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = json.dumps({"topic": f"topic-{subject}-{req.context['service']}"})

class DojotTenants:
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = json.dumps({"tenants": ["admin"]})


class AuthMiddleware(object):
    def process_request(self, req, resp):
        rawToken = req.get_header('Authorization')
        if rawToken:
            token = rawToken.split(".")
            missing_padding = 4 - (len(token[1]) % 4)
            token[1] += "=" * missing_padding
            tokenData = json.loads(base64.b64decode(token[1]).decode("utf-8"))
            req.context["service"] = tokenData["service"]



logs = DojotLogs()
topics = DojotTopics()

LOGGER = Log().color_log()
def receive_message_a(tenant, data):
    if tenant not in logs.messages["subjectA"]:
        logs.messages["subjectA"][tenant] = []
    logs.messages["subjectA"][tenant].append(data)
    print(f"Current message logs is {logs.messages}")

def receive_message_b(tenant, data):
    if tenant not in logs.messages["subjectB"]:
        logs.messages["subjectB"][tenant] = []
    logs.messages["subjectB"][tenant].append(data)
    print(f"Current message logs is {logs.messages}")

def receive_message_service_status(tenant, data):
    if tenant not in logs.messages["serviceStatus"]:
        logs.messages["serviceStatus"][tenant] = []
    logs.messages["serviceStatus"][tenant].append(data)
    print(f"Current message logs is {logs.messages}")

def create_messenger():
    config = Config()
    messenger = Messenger("Dojot-Snoop", config)
    return messenger

def init_routes(app, messenger):
    app.add_route('/logs', logs)
    app.add_route('/send', DojotSend(messenger))
    app.add_route('/topic/{subject}', DojotTopics())
    app.add_route('/admin/tenants', DojotTenants())

def init_messenger(messenger):
    messenger.init()
    messenger.create_channel("subjectA", "rw")
    messenger.create_channel("subjectB", "rw")
    messenger.create_channel("serviceStatus", "rw")
    logs.messages["subjectA"] = {}
    logs.messages["subjectB"] = {}
    logs.messages["serviceStatus"] = {}

    messenger.on("subjectA", "message", receive_message_a)
    messenger.on("subjectB", "message", receive_message_b)
    messenger.on("serviceStatus", "message", receive_message_service_status)

class HTTPServer(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.httpd = simple_server.make_server('127.0.0.1', 5002, app)

    def run(self):
        self.httpd.serve_forever()


if __name__ == '__main__':
    app = falcon.API(middleware=[AuthMiddleware()])
    messenger = create_messenger()
    init_routes(app, messenger)

    httpServerThr = HTTPServer(app)
    httpServerThr.start()

    init_messenger(messenger)