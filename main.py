from dojotmodulepython import config
from dojotmodulepython.kafka import Consumer
from dojotmodulepython.kafka import TopicManager
from dojotmodulepython.kafka import Producer
from dojotmodulepython import auth
from dojotmodulepython import Messenger
import time

import signal
import os
def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        os._exit(1)
signal.signal(signal.SIGINT, signal_handler)

def does_nothing(tenant,data):
    print("[SUCCESS] received message!!! tenant: %s, data: %s" % (tenant,data))

def main():
    print(config.dojot['subjects']['tenancy'])
    messenger = Messenger("Matheus")
    messenger.init()
    print("\n")
    messenger.create_channel(config.dojot['subjects']['device_data'], "rw")
    
    messenger.on("device-data","message",does_nothing)
    # time.sleep(1)

    print("\n")
    # messenger.publish(config.dojot['subjects']['device_data'], "admin", {
    #     "metadata": {
    #         "deviceid": "c6ea4b",
    #         "tenant": "admin",
    #         "timestamp": 1528226137452,
    #         "templates": [2, 3]
    #     },
    #     "attrs": {
    #         "humidity": 60
    #     }
    # })

    msgid=1
    while True:
        messenger.publish(config.dojot['subjects']['device_data'], "admin", {
            # "metadata": {
            #     "deviceid": "c6ea4b",
            #     "tenant": "admin",
            #     "timestamp": 1528226137452,
            #     "templates": [2, 3]
            # },
            "attrs": {
                "humidity": msgid
            }
        })
    # messenger.publish(config.dojot['subjects']['tenancy'],config.dojot['management_service'],{"tenant": "matheus"})
        time.sleep(1)
        msgid = msgid + 1
    os._exit(0)


if __name__ == "__main__":
    main()
