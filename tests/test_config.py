import os
import pytest
from dojot.module import Config

def assert_config_creation(data=None):
    config = Config(data)
    assert config is not None
    assert config.kafka is not None
    assert config.data_broker is not None
    assert config.auth is not None
    assert config.device_manager is not None
    assert config.dojot is not None
    return config

def assert_kafka_config(config):
    assert "producer" in config.kafka
    assert "client.id" in config.kafka["producer"]
    assert "bootstrap_servers" in config.kafka["producer"]
    assert "consumer" in config.kafka
    assert "group_id" in config.kafka["consumer"]
    assert "bootstrap_servers" in config.kafka["consumer"]
    assert "poll_timeout" in config.kafka["dojot"]

def assert_services_config(config):
    assert "url" in config.data_broker
    assert "timeout_sleep" in config.data_broker
    assert "connection_retries" in config.data_broker
    assert "url" in config.auth
    assert "timeout_sleep" in config.auth
    assert "connection_retries" in config.auth
    assert "url" in config.device_manager
    assert "timeout_sleep" in config.device_manager
    assert "connection_retries" in config.device_manager

def assert_dojot_config(config):
    assert "management" in config.dojot
    assert "user" in config.dojot["management"]
    assert "tenant" in config.dojot["management"]
    assert "subjects" in config.dojot

    assert "tenancy" in config.dojot["subjects"]
    assert "devices" in config.dojot["subjects"]
    assert "device_data" in config.dojot["subjects"]

def assert_default_config(config):
    assert_kafka_config(config)
    assert_services_config(config)
    assert_dojot_config(config)

def assert_extra_kafka_config(config):
    assert "extra-config-p" in config.kafka["producer"]
    assert "extra-data-producer" == config.kafka["producer"]["extra-config-p"]
    assert "extra-config-c" in config.kafka["consumer"]
    assert "extra-data-consumer" == config.kafka["consumer"]["extra-config-c"]

def assert_extra_services_config(config):
    assert "extra-dbroker" in config.data_broker
    assert "data-dbroker" == config.data_broker["extra-dbroker"]
    assert "extra-auth" in config.auth
    assert "data-auth" == config.auth["extra-auth"]
    assert "extra-device-manager" in config.device_manager
    assert "data-device-manager" == config.device_manager["extra-device-manager"]

def assert_extra_dojot_config(config):
    assert "extra-subject" in config.dojot["subjects"]
    assert "extra-subject-name" == config.dojot["subjects"]["extra-subject"]

def test_default_config():
    config = assert_config_creation()
    assert_default_config(config)

def test_custom_config():
    kafka_data = {
        "kafka": {
            "producer": {
                "client.id":  "producer-id",
                "bootstrap_servers": ["kafka:9092"],
                "extra-config-p": "extra-data-producer"
            },
            "consumer": {
                "group.id":  "consumer-group",
                "bootstrap_servers": ["kafka:9092"],
                "extra-config-c": "extra-data-consumer"
            },
            "dojot": {
                "poll_timeout": 2000,
                "subscription_holdoff": 2.5,
                "extra-config-kafka-dojot": "extra"
            }
        }
    }
    services_data = {
        "data_broker" : {
            "url" : "localhost:8080",
            "timeout_sleep": 5,
            "connection_retries": 3,
            "extra-dbroker": "data-dbroker"
        },
        "auth": {
            "url": "localhost:5000",
            "timeout_sleep": 5,
            "connection_retries": 3,
            "extra-auth": "data-auth"
        },
        "device_manager": {
            "url": "http://device-manager:5000",
            "timeout_sleep": 5,
            "connection_retries": 3,
            "extra-device-manager": "data-device-manager"
        }
    }

    dojot_data = {
        "dojot": {
            "management": {
                "user" : "dojot-management",
                "tenant": "dojot-management"
            },
            "subjects": {
                "tenancy": "dojot.tenancy",
                "devices": "dojot.device-manager.device",
                "device_data": "device-data",
                "extra-subject" : "extra-subject-name"
            }
        }
    }
    config = assert_config_creation(kafka_data)
    assert_default_config(config)
    assert_extra_kafka_config(config)

    config = assert_config_creation(services_data)
    assert_default_config(config)
    assert_extra_services_config(config)

    config = assert_config_creation(dojot_data)
    assert_default_config(config)
    assert_extra_dojot_config(config)


def test_env_config():
    os.environ["KAFKA_HOSTS"] = "local-kafka1:9092,local-kafka2:9092"
    os.environ["DOJOT_KAFKA_SUBSCRIPTION_HOLDOFF"] = "1234"
    os.environ["DOJOT_KAFKA_POLL_TIMEOUT"] = "4321"
    os.environ["KAFKA_GROUP_ID"] = "local-kafka"
    os.environ['DATA_BROKER_URL'] = "http://local-data-broker"
    os.environ['DEVICE_MANAGER_URL'] = "http://local-device-manager"
    os.environ['AUTH_URL'] = "http://local-auth"
    os.environ['DOJOT_MANAGEMENT_USER'] = "local-mgmt"
    os.environ['DOJOT_MANAGEMENT_TENANT'] = "local-tenant"
    os.environ['DOJOT_SUBJECT_TENANCY'] = "dojot.local.tenancy"
    os.environ['DOJOT_SUBJECT_DEVICES'] = "dojot.local.devices"
    os.environ['DOJOT_SUBJECT_DEVICE_DATA'] = "dojot.local.data"

    data = {
        "kafka": {
            "producer": {
                "client.id":  "kafka",
                "bootstrap_servers": ["local-kafka1:9092", "local-kafka2:9092"],
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
                "group_id":  "local-kafka",
                "bootstrap_servers": ["local-kafka1:9092", "local-kafka2:9092"],
            },
            "dojot": {
                "poll_timeout": 4321,
                "subscription_holdoff": 1234
            }
        },
        "data_broker" : {
            "url" : "http://local-data-broker",
            "timeout_sleep": 5,
            "connection_retries": 3,
        },
        "auth": {
            "url": "http://local-auth",
            "timeout_sleep": 5,
            "connection_retries": 3
        },
        "device_manager": {
            "url": "http://local-device-manager",
            "timeout_sleep": 5,
            "connection_retries": 3
        },
        "dojot": {
            "management": {
                "user" : "local-mgmt",
                "tenant": "local-tenant"
            },
            "subjects": {
                "tenancy": "dojot.local.tenancy",
                "devices": "dojot.local.devices",
                "device_data": "dojot.local.data"
            }
        }
    }

    config = assert_config_creation()
    assert config.kafka == data["kafka"]
    assert config.auth == data["auth"]
    assert config.device_manager == data["device_manager"]
    assert config.data_broker == data["data_broker"]
    assert config.dojot == data["dojot"]