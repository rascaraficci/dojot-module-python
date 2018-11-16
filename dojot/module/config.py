"""
Configuration data module
"""

import os
import yaml

class Config:
    """
    Main configuration class

    This class contains all needed configuration for this library
    """
    def __init__(self, config=None):
        """
        Config constructor

        :type config: dict or None
        :param config: A configuration dictionary. If set, all its attributes
            will be set to this object.

        Any top level key will overwrite the default configuration, i.e.,
        setting a `kafka` object to config param will overwrite all Kafka
        configuration. An example of such dictionary is:

        .. code-block:: python

            config = {
                "kafka" : {
                    "producer": {
                        "client.id": "kafka",
                        "metadata.broker.list": "kafka:9092",
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
                        "group.id": "my-module",
                        "metadata.broker.list": "kafka:9092"
                    }
                }
                "data_broker" : {
                    "url": "http://data-broker"
                }

                "auth" : {
                    "url": "http://auth:5000"
                }

                "dojot" : {
                    "management_service": "dojot-management",
                    "subjects": {
                        "tenancy": "dojot.tenancy",
                        "devices": "dojot.device-manager.device",
                        "device_data": "device-data,
                    }
                }
            }

        .. warning::
        
            If set, the `dojot` section should be in sync with all other
            modules. Otherwise this module won't work properly.

        .. note::
        
            The Kafka object is straight from librdkafka configuration,
            separated into producer and consumer subobjects. For more
            information about this configuration, you should check its
            documentation.

        """
        self.load_defaults()
        self.load_env()
        if config is not None:
            if "kafka" in config: self.kafka = config["kafka"]
            if "data_broker" in config: self.data_broker = config["data_broker"]
            if "auth" in config: self.auth = config["auth"]
            if "dojot" in config: self.dojot = config["dojot"]


    def load_defaults(self):
        """
        Load default configuration, which is:

        .. code-block:: yaml

            kafka:
                producer:
                    client.id: "kafka"
                    metadata.broker.list: "kafka:9092"
                    compression.codec: "gzip"
                    retry.backoff.ms: 200
                    message.send.max.retries: 10
                    socket.keepalive.enable: True
                    queue.buffering.max.messages: 100000
                    queue.buffering.max.ms: 1000
                    batch.num.messages: 1000000
                    dr_cb: true
                consumer:
                    group.id: "my-module"
                    metadata.broker.list: "kafka:9092"
            data_broker:
                url: "http://data-broker"
            auth:
                url: "http://auth:5000"
            dojot:
                management_service: "dojot-management"
                subjects:
                    tenancy: "dojot.tenancy"
                    devices: "dojot.device-manager.device"
                    device_data: "device-data"

        .. warning:: 

            Calling this function will overwrite any previously set
            configuration in the created object. Also setting any configuration
            *after* Kafka is started or any Messenger object is created will
            have no effect on them.

        .. warning:: 
        
            If set, the `dojot` section should be in sync with all other
            modules. Otherwise this module won't work properly.

        """
        self.kafka = {
            "producer": {
                "client.id": "kafka",
                "metadata.broker.list": "kafka:9092",
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
                "group.id": "my-module",
                "metadata.broker.list": "kafka:9092"
            }
        }
        self.data_broker = {
            "url": "http://data-broker"
        }

        self.auth = {
            "url": "http://auth:5000"
        }

        self.dojot = {
            "management_service": "dojot-management",
            "subjects": {
                "tenancy": "dojot.tenancy",
                "devices": "dojot.device-manager.device",
                "device_data": "device-data"
            }
        }


    def load_file(self, config_file):
        """
        Load configuration from a file. 

        Any top level key will overwrite the default configuration, i.e., having
        a `kafka` item in the file will overwrite all Kafka configuration.
        Such file should have the following sections:

        .. code-block:: yaml

            kafka:
                producer:
                    client.id: "kafka"
                    metadata.broker.list: "kafka:9092"
                consumer:
                    group.id: "data-broker"
                    metadata.broker.list: "kafka:9092"
                data_broker:
                    url: "http://data-broker"
                auth:
                    url: "http://auth:5000"
                dojot:
                    management_service: "dojot-management"
                    subjects:
                        tenancy: "dojot.tenancy"
                        devices: "dojot.device-manager.device"
                        device_data: "device-data"

        .. warning::

            Calling this function will overwrite any previously set
            configuration in the created object. Also setting any configuration
            *after* Kafka is started or any Messenger object is created will
            have no effect on them.
        
        .. warning::
        
            If set, the `dojot` section should be in sync with all other
            modules. Otherwise this module won't work properly.

        .. note::
        
            The Kafka object is straight from librdkafka configuration,
            separated into producer and consumer subobjects. For more
            information about this configuration, you should check its
            documentation.

        """
        self.load_defaults()
        parsed_config = yaml.safe_load(open(config_file))
        print(f"Parsed config: {parsed_config}")

        if "kafka" in parsed_config: print("kafka")
        if "data_broker" in parsed_config: print("data_broker")
        if "auth" in parsed_config: print("auth")
        if "dojot" in parsed_config: print("dojot")

        if "kafka" in parsed_config: self.kafka = parsed_config["kafka"]
        if "data_broker" in parsed_config: self.data_broker = parsed_config["data_broker"]
        if "auth" in parsed_config: self.auth = parsed_config["auth"]
        if "dojot" in parsed_config: self.dojot = parsed_config["dojot"]

        self.load_env()

    def load_env(self):
        """
        Load configuration from environment variables.

        Any environment variable will overwrite the default configuration.
        Check load_defaults() function.

        The list of envirnoment variables is:

        - ``KAFKA_HOSTS``: a comma-separated list of hosts where an instance
          of Kafka is running. This will affect the `metadata.broker.list`
          parameter for both Kafka consumer and producer.
        - ``KAFKA_GROUP_ID``: The Kafka consumer group ID to be used.
        - ``DATA_BROKER_URL``: Where DataBroker service can be reached.
        - ``AUTH_URL``: Where Auth service can be reached.
        - ``DOJOT_SERVICE_MANAGEMENT``: service to be used when asking
          DataBroker for management topics (such as tenancy-related topics)
        - ``DOJOT_SUBJECT_TENANCY``: Subject to be used when asking
          DataBroker for tenancy topics.
        - ``DOJOT_SUBJECT_DEVICES``: Subject to be used when asking
          DataBroker for device topics.
        - ``DOJOT_SUBJECT_DEVICE_DATA``: Subject to be used when asking
          DataBroker for device data topics.

        """

        self.kafka["producer"]["metadata.broker.list"] = os.environ.get(
            'KAFKA_HOSTS', self.kafka["producer"]["metadata.broker.list"])

        self.kafka["consumer"]["metadata.broker.list"] = os.environ.get(
            'KAFKA_HOSTS', self.kafka["consumer"]["metadata.broker.list"])

        self.kafka["consumer"]["group.id"] = os.environ.get(
            'KAFKA_GROUP_ID', self.kafka["consumer"]["group.id"])

        self.data_broker["url"] = os.environ.get(
            'DATA_BROKER_URL', self.data_broker["url"])

        self.auth["url"] = os.environ.get('AUTH_URL', self.auth["url"])

        self.dojot["management_service"] = os.environ.get(
            'DOJOT_SERVICE_MANAGEMENT', self.dojot["management_service"])

        self.dojot["subjects"]["tenancy"] = os.environ.get(
            'DOJOT_SUBJECT_TENANCY', self.dojot["subjects"]["tenancy"])

        self.dojot["subjects"]["devices"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICES', self.dojot["subjects"]["devices"])

        self.dojot["subjects"]["device_data"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICE_DATA', self.dojot["subjects"]["device_data"])
