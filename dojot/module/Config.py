import os

class Config:

    def __init__(self):
        self.kafka = {
            "producer": {
                "client.id": "kafka",
                "metadata.broker.list": os.environ.get('KAFKA_HOSTS', "kafka:9092"),
                "compression.codec": "gzip",
                "retry.backoff.ms": 200,
                "message.send.max.retries": 10,
                "socket.keepalive.enable": True,
                "queue.buffering.max.messages": 100000,
                "queue.buffering.max.ms": 1000,
                "batch.num.messages": 1000000,
                "dr_cb": True
            },
            "consumer": {
                "group.id": os.environ.get('KAFKA_GROUP_ID', "data-broker"),
                "metadata.broker.list": os.environ.get('KAFKA_HOSTS', "kafka:9092")
            }
        }
        self.data_broker = {
            "url": os.environ.get('DATA_BROKER_URL', "http://data-broker")
        }

        self.auth = {
            "url": os.environ.get('AUTH_URL', "http://auth:5000")
        }

        self.dojot = {
            "management_service": os.environ.get('DOJOT_SERVICE_MANAGEMENT', "dojot-management"),
            "subjects": {
                "tenancy": os.environ.get('DOJOT_SUBJECT_TENANCY', "dojot.tenancy"),
                "devices": os.environ.get('DOJOT_SUBJECT_DEVICES', "dojot.device-manager.device"),
                "device_data": os.environ.get('DOJOT_SUBJECT_DEVICE_DATA', "device-data")
            }
        }


config = Config()
