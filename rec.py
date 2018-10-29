from dojotmodulepython import config
from dojotmodulepython.kafka import Consumer
from dojotmodulepython.kafka import TopicManager
from dojotmodulepython.kafka import Producer
from dojotmodulepython import auth
from dojotmodulepython import Messenger

def does_nothing(tenant,data):
    print("[SUCCESS] received message!!! tenant: %s, data: %s" % (tenant,data))

def main():
    print(config.dojot['subjects']['tenancy'])
    messenger = Messenger("Matheus")
    messenger.init()
    print("\n")
    messenger.create_channel(config.dojot['subjects']['device_data'], "passwd")
    messenger.on("device-data","message",does_nothing,True)


if __name__ == "__main__":
    main()
