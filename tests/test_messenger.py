import pytest
import uuid
import json
from unittest.mock import Mock, patch, ANY
from dojot.module import Messenger, Auth
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

def test_messenger():
    config = Mock(dojot={
        "subjects": {
            "tenancy": "sample-tenancy-subject"
        }
    })

    patchCreateCh = patch("dojot.module.Messenger.create_channel")
    patchConsumer = patch("dojot.module.kafka.Consumer.__init__", return_value=None)
    patchProducerInit = patch("dojot.module.kafka.Producer.init", return_value=None)
    patchProducer = patch("dojot.module.kafka.Producer.__init__", return_value=None)
    patchUuid = patch("uuid.uuid4", return_value="1234")
    patchTopicManager = patch("dojot.module.kafka.TopicManager.__init__", return_value=None)

    with patchUuid, patchTopicManager, patchCreateCh as mockCreateCh, patchConsumer as mockConsumer, patchProducerInit as mockProducerInit, patchProducer as mockProducer:
        messenger = Messenger("sample-messenger", config)
        mockCreateCh.assert_called_once_with("sample-tenancy-subject", "rw", True)
        mockProducer.assert_called_once()
        mockConsumer.assert_called_once()
        mockProducerInit.assert_called_once()
        assert messenger.instance_id == "sample-messenger1234"

    with patch("dojot.module.kafka.Producer.init", return_value=10):
        # This should trigger an error
        pass


def test_messenger_init():
    patchMessengerOn = patch("dojot.module.Messenger.on")
    # patchAuth = patch("dojot.module.Auth.__init__", return_value=None)
    patchMessengerProcess = patch("dojot.module.Messenger.process_new_tenant")
    # patchGetTenants = patch("dojot.module.Auth.get_tenants", return_value=["tenant1", "tenant2"])

    config = Mock(dojot={
        "subjects": {
            "tenancy": "sample-tenancy-subject"
        },
        "management": {
            "tenant": "sample-management-tenant"
        }
    })

    def reset_scenario():
        mockMessengerProcess.reset_mock()

    mockSelf = Mock(config=config, tenants=[], auth=Mock(get_tenants=Mock(return_value=["tenant1", "tenant2"])))
    with patchMessengerOn, patchMessengerProcess as mockMessengerProcess:
        mockSelf.process_new_tenant = mockMessengerProcess
        Messenger.init(mockSelf)
        mockMessengerProcess.assert_any_call('sample-management-tenant', '{"tenant": "tenant1"}')
        mockMessengerProcess.assert_any_call('sample-management-tenant', '{"tenant": "tenant2"}')

    reset_scenario()
    # patchGetTenants = patch("dojot.module.Auth.get_tenants", return_value=None)
    mockSelf = Mock(config=config, tenants=[], auth=Mock(get_tenants=Mock(return_value=None)))
    with pytest.raises(UserWarning), patchMessengerOn, patchMessengerProcess as mockMessengerProcess:
        mockSelf.process_new_tenant = mockMessengerProcess
        Messenger.init(mockSelf)
        mockMessengerProcess.assert_not_called()


def test_messenger_process_new_tenant():
    config = Mock(dojot={
        "subjects": {
            "tenancy": "sample-tenancy-subject"
        },
        "management": {
            "tenant": "sample-management-tenant"
        }
    })

    mockSelf = Mock(
        tenants=[],
        subjects={},
        config=config,
        _Messenger__bootstrap_tenants=Mock(),
        emit=Mock())

    def reset_scenario():
        mockSelf._Messenger__bootstrap_tenants.reset_mock()
        mockSelf.emit.reset_mock()
        mockSelf.tenants = []
        mockSelf.subjects = {}

    # Happy day route, no registered subjects
    reset_scenario()
    Messenger.process_new_tenant(mockSelf,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mockSelf._Messenger__bootstrap_tenants.assert_not_called()
    mockSelf.emit.assert_called_once_with(
        "sample-tenancy-subject",
        "sample-management-tenant",
        "new-tenant",
        "sample-tenant")

    # Happy day route, one registered subject
    reset_scenario()
    mockSelf.subjects = {
        "sample-subject": {
            "mode": "sample-subject-mode"
        }
    }
    Messenger.process_new_tenant(mockSelf,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mockSelf._Messenger__bootstrap_tenants.assert_called_once_with("sample-subject",
        "sample-tenant",
        "sample-subject-mode")
    mockSelf.emit.assert_called_once_with(
        "sample-tenancy-subject",
        "sample-management-tenant",
        "new-tenant",
        "sample-tenant")


    # Invalid payload
    reset_scenario()
    Messenger.process_new_tenant(mockSelf,
        "sample-tenant",
        'wrong-payload')
    mockSelf._Messenger__bootstrap_tenants.assert_not_called()
    mockSelf.emit.assert_not_called()


    # Missing tenant in data
    reset_scenario()
    Messenger.process_new_tenant(mockSelf,
        "sample-tenant",
        '{"key" : "not-expected"}')
    mockSelf._Messenger_bootstrap_tenants.assert_not_called()
    mockSelf.emit.assert_not_called()

    # Tenant already bootstrapped
    reset_scenario()
    mockSelf.tenants = ["sample-tenant"]
    Messenger.process_new_tenant(mockSelf,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mockSelf._Messenger__bootstrap_tenants.assert_not_called()
    mockSelf.emit.assert_not_called()

def test_messenger_emit():

    mockSelf = Mock(
        event_callbacks={}
    )

    mockCallback = Mock()

    def reset_scenario():
        mockSelf.event_callbacks = {
            "registered-subject": {
                "registered-event": [mockCallback]
            }
        }

    # No registered subject nor event
    reset_scenario()
    Messenger.emit(mockSelf,
        "sample-subject",
        "sample-tenant",
        "sample-event",
        "sample-data")
    mockCallback.assert_not_called()


    # Registered subject, but not event
    reset_scenario()
    Messenger.emit(mockSelf,
        "registered-subject",
        "sample-tenant",
        "sample-event",
        "sample-data")
    mockCallback.assert_not_called()

    # Not registered subject, but registered event
    reset_scenario()
    Messenger.emit(mockSelf,
        "sample-subject",
        "sample-tenant",
        "registered-event",
        "sample-data")
    mockCallback.assert_not_called()

    # Registered subject and event
    reset_scenario()
    Messenger.emit(mockSelf,
        "registered-subject",
        "sample-tenant",
        "registered-event",
        "sample-data")
    mockCallback.assert_called_once_with("sample-tenant", "sample-data")


def test_messenger_on():

    mockSelf = Mock(
        event_callbacks={},
        create_channel=Mock(),
        subjects={},
        global_subjects={}
    )

    def reset_scenario():
        mockSelf.event_callbacks = {}
        mockSelf.subjects = {}
        mockSelf.global_subjects = {}
        mockSelf.create_channel.reset_mock()

    # No registered subjects
    Messenger.on(mockSelf, "sample-subject", "sample-event", "callback-dummy")
    assert "sample-subject" in mockSelf.event_callbacks
    assert "sample-event" in mockSelf.event_callbacks["sample-subject"]
    assert "callback-dummy" in mockSelf.event_callbacks["sample-subject"]["sample-event"]
    mockSelf.create_channel.assert_called_once_with("sample-subject")

    # Registered a local subject
    reset_scenario()
    mockSelf.subjects = ["sample-subject"]
    Messenger.on(mockSelf, "sample-subject", "sample-event", "callback-dummy")
    mockSelf.create_channel.assert_not_called()

    # Registered a global subject
    reset_scenario()
    mockSelf.global_subjects = ["sample-subject"]
    Messenger.on(mockSelf, "sample-subject", "sample-event", "callback-dummy")
    mockSelf.create_channel.assert_not_called()

def test_messenger_bootstrap_tenants():

    mockSelf = Mock(
        topic_manager=Mock(
            get_topic=Mock()
        ),
        topics={},
        consumer=Mock(
            subscribe=Mock(),
            start=Mock()
        ),
        producer_topics={}
    )

    def reset_scenario():
        mockSelf.topic_manager.get_topic.reset_mock()
        mockSelf.topics = {}
        mockSelf.consumer.subscribe.reset_mock()
        mockSelf.consumer.start.reset_mock()
        mockSelf.producer_topics = {}

    # Happy day, non-global
    mockSelf.topic_manager.get_topic.return_value = "dummy-topic"
    Messenger._Messenger__bootstrap_tenants(mockSelf, "sample-subject", "sample-tenant", "rw")
    mockSelf.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    assert "dummy-topic" in mockSelf.topics
    mockSelf.consumer.subscribe.assert_called_once_with("dummy-topic", ANY)
    mockSelf.consumer.start.assert_called_once()
    assert "sample-subject" in mockSelf.producer_topics
    assert "sample-tenant" in mockSelf.producer_topics["sample-subject"]

    # Topic manager returned none
    reset_scenario()
    mockSelf.topic_manager.get_topic.return_value = None
    Messenger._Messenger__bootstrap_tenants(mockSelf, "sample-subject", "sample-tenant", "rw")
    mockSelf.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    assert "dummy-topic" not in mockSelf.topics
    mockSelf.consumer.subscribe.assert_not_called()
    mockSelf.consumer.start.assert_not_called()
    assert "sample-subject" not in mockSelf.producer_topics

    # Already have such topic
    reset_scenario()
    mockSelf.topics = ["dummy-topic"]
    mockSelf.topic_manager.get_topic.return_value = "dummy-topic"
    Messenger._Messenger__bootstrap_tenants(mockSelf, "sample-subject", "sample-tenant", "rw")
    mockSelf.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    mockSelf.consumer.subscribe.assert_not_called()
    mockSelf.consumer.start.assert_not_called()
    assert "sample-subject" not in mockSelf.producer_topics

def test_messenger_process_kafka_messages():
    mockSelf = Mock(
        emit=Mock(),
        topics={}
    )

    def reset_scenario():
        mockSelf.emit.reset_mock()
        mockSelf.topics={}

    # Happy day
    mockSelf.topics = {
        "sample-topic": {
            "subject": "sample-subject",
            "tenant": "sample-tenant"
        }
    }
    Messenger._Messenger__process_kafka_messages(mockSelf, "sample-topic", "sample-message")
    mockSelf.emit.assert_called_once_with("sample-subject", "sample-tenant", "message", "sample-message")

    # No registered topic
    reset_scenario()
    Messenger._Messenger__process_kafka_messages(mockSelf, "sample-topic", "sample-message")
    mockSelf.emit.assert_not_called()

def test_messenger_publish():
    mockSelf = Mock(
        producer_topics={},
        producer=Mock(
            produce=Mock()
        )
    )

    def reset_scenario():
        mockSelf.producer_topics={}
        mockSelf.producer.produce.reset_mock()

    # Happy day
    mockSelf.producer_topics = {
        "sample-subject": {
            "sample-tenant": "sample-topic"
        }
    }
    Messenger.publish(mockSelf, "sample-subject", "sample-tenant", "sample-message")
    mockSelf.producer.produce.assert_called_once_with("sample-topic", "sample-message")

    # No registered subject
    reset_scenario()
    Messenger.publish(mockSelf, "sample-subject", "sample-tenant", "sample-message")
    mockSelf.producer.produce.assert_not_called()

    # No registered tenant
    mockSelf.producer_topics = {
        "sample-subject": {
            "other-tenant": "sample-topic"
        }
    }
    Messenger.publish(mockSelf, "sample-subject", "sample-tenant", "sample-message")
    mockSelf.producer.produce.assert_not_called()



def test_messenger_request_device():
    mockSelf = Mock(
        config=Mock(
            device_manager={
                "url": "http://sample-url",
                "connection_retries": 3,
                "timeout_sleep": 10
            },
            dojot={
                "subjects": {
                    "devices": "devices-subject"
                }
            }
        ),
        auth=Mock(
            get_access_token=Mock(
                return_value="super-secret-token"
            )
        ),
        emit=Mock()
    )
    mockResponse = Mock(
        json=Mock(return_value={
            "devices": ["device-1", "device-2"],
            "pagination": {
                "has_next": False
            }
        }),
        text="there was an error, but I will not tell you.",
        status_code=200
    )

    def reset_scenario():
        mockSelf.emit.reset_mock()

    patchReqGet = patch("requests.get", return_value=mockResponse)
    patchSleep = patch("time.sleep")

    with patchSleep, patchReqGet as mockReqRequest:
        Messenger.request_device(mockSelf, "sample-tenant")
        mockReqRequest.assert_called_with("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })

        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-1\"}")
        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-2\"}")

    reset_scenario()

    mockResponse.status_code = 301
    patchReqGet = patch("requests.get", return_value=mockResponse)

    with patchSleep, patchReqGet as mockReqRequest:
        Messenger.request_device(mockSelf, "sample-tenant")
        mockReqRequest.assert_called_with("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })

        mockSelf.emit.assert_not_called()

    reset_scenario()

    mockResponse.status_code = 200
    mockResponse.json.return_value = {
        "devices": ["device-1", "device-2"],
        "pagination": {
            "has_next": True,
            "next_page": 199
        }
    }
    mockResponse2 =  Mock(
        json=Mock(return_value={
            "devices": ["device-3", "device-4"],
            "pagination": {
                "has_next": False
            }
        }),
        text="there was an error, but I will not tell you.",
        status_code=200
    )
    patchReqGet = patch("requests.get", side_effect=[mockResponse, mockResponse2])

    with patchSleep, patchReqGet as mockReqRequest:
        Messenger.request_device(mockSelf, "sample-tenant")
        mockReqRequest.assert_any_call("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })
        mockReqRequest.assert_any_call("http://sample-url/device?page_num=199", headers={
            'authorization': "Bearer super-secret-token"
        })
        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-1\"}")
        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-2\"}")
        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-3\"}")
        mockSelf.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-4\"}")


    def assert_exceptions(patchSleep, patchReqGet):
        with patchSleep, patchReqGet as mockReqRequest:
            Messenger.request_device(mockSelf, "sample-tenant")
            mockReqRequest.assert_any_call("http://sample-url/device", headers={
                'authorization': "Bearer super-secret-token"
            })
            mockSelf.emit.assert_not_called()

    reset_scenario()
    patchReqGet = patch("requests.get", side_effect=ConnectionError)
    assert_exceptions(patchSleep, patchReqGet)

    reset_scenario()
    patchReqGet = patch("requests.get", side_effect=Timeout)
    assert_exceptions(patchSleep, patchReqGet)

    reset_scenario()
    patchReqGet = patch("requests.get", side_effect=TooManyRedirects)
    assert_exceptions(patchSleep, patchReqGet)

    reset_scenario()
    patchReqGet = patch("requests.get", side_effect=ValueError)
    assert_exceptions(patchSleep, patchReqGet)

def test_messenger_generate_device_events():
    mockSelf = Mock(
        tenants=["tenant-1", "tenant-2"],
        request_device=Mock()
    )

    Messenger.generate_device_create_event_for_active_devices(mockSelf)
    mockSelf.request_device.assert_any_call("tenant-1")
    mockSelf.request_device.assert_any_call("tenant-2")


def test_messenger_shutdown():
    mockSelf = Mock(
        consumer=Mock(stop=Mock())
    )

    Messenger.shutdown(mockSelf)
    mockSelf.consumer.stop.assert_called_once()

