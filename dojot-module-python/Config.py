import os


class Config:

    def __init__(self):
        self.kafka = {
            "address": os.environ.get('KAFKA_ADDRESS', "kafka"),
            "port": os.environ.get('KAFKA_PORT', "9092"),
            "host": os.environ.get('KAFKA_HOST',"kafka:9092"),
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
            "host": os.environ.get('DATA_BROKER_HOST', "http://data-broker")
        }

        self.auth = {
            "host": os.environ.get('AUTH_HOST', "http://auth:5000")
        }

        self.device_manager = {
            "host": os.environ.get('DEVICE_MANAGER_HOST', "http://device-manager:5000")
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
