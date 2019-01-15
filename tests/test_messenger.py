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

    patch_create_ch = patch("dojot.module.Messenger.create_channel")
    patch_producer_init = patch("dojot.module.kafka.Producer.init", return_value=None)
    patch_producer = patch("dojot.module.kafka.Producer.__init__", return_value=None)
    patch_uuid = patch("uuid.uuid4", return_value="1234")
    patch_topic_manager = patch("dojot.module.kafka.TopicManager.__init__", return_value=None)

    with patch_uuid, patch_topic_manager, patch_create_ch, patch_producer_init as mock_producer_init, patch_producer as mock_producer:
        messenger = Messenger("sample-messenger", config)
        mock_producer.assert_called_once()
        mock_producer_init.assert_called_once()
        assert messenger.instance_id == "sample-messenger1234"

def test_messenger_init():
    patch_messenger_on = patch("dojot.module.Messenger.on")
    # patchAuth = patch("dojot.module.Auth.__init__", return_value=None)
    patch_messenger_process = patch("dojot.module.Messenger.process_new_tenant")
    patch_consumer = patch("dojot.module.kafka.Consumer.__init__", return_value=None)
    # patchGetTenants = patch("dojot.module.Auth.get_tenants", return_value=["tenant1", "tenant2"])

    config = Mock(dojot={
        "subjects": {"tenancy": "sample-tenancy-subject"},
        "management": {"tenant": "sample-management-tenant"}
    },
    kafka={
        "dojot": {"poll_timeout": 1, "subscription_holdoff": 1},
        "consumer": {},
        "producer": {}
    }
    )

    def reset_scenario():
        mock_messenger_process.reset_mock()

    mock_self = Mock(config=config, tenants=[], auth=Mock(get_tenants=Mock(return_value=["tenant1", "tenant2"])))
    with patch_messenger_on, patch_consumer as mock_consumer, patch_messenger_process as mock_messenger_process:
        mock_self.process_new_tenant = mock_messenger_process
        Messenger.init(mock_self)
        mock_consumer.assert_called()
        mock_messenger_process.assert_any_call('sample-management-tenant', '{"tenant": "tenant1"}')
        mock_messenger_process.assert_any_call('sample-management-tenant', '{"tenant": "tenant2"}')

    reset_scenario()
    # patchGetTenants = patch("dojot.module.Auth.get_tenants", return_value=None)
    mock_self = Mock(config=config, tenants=[], auth=Mock(get_tenants=Mock(return_value=None)))
    with pytest.raises(UserWarning), patch_messenger_on, patch_messenger_process as mock_messenger_process:
        mock_self.process_new_tenant = mock_messenger_process
        Messenger.init(mock_self)
        mock_messenger_process.assert_not_called()


def test_messenger_process_new_tenant():
    config = Mock(dojot={
        "subjects": {
            "tenancy": "sample-tenancy-subject"
        },
        "management": {
            "tenant": "sample-management-tenant"
        }
    })

    mock_self = Mock(
        tenants=[],
        subjects={},
        config=config,
        _Messenger__bootstrap_tenants=Mock(),
        emit=Mock())

    def reset_scenario():
        mock_self._Messenger__bootstrap_tenants.reset_mock()
        mock_self.emit.reset_mock()
        mock_self.tenants = []
        mock_self.subjects = {}

    # Happy day route, no registered subjects
    reset_scenario()
    Messenger.process_new_tenant(mock_self,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mock_self._Messenger__bootstrap_tenants.assert_not_called()
    mock_self.emit.assert_called_once_with(
        "sample-tenancy-subject",
        "sample-management-tenant",
        "new-tenant",
        "sample-tenant")

    # Happy day route, one registered subject
    reset_scenario()
    mock_self.subjects = {
        "sample-subject": {
            "mode": "sample-subject-mode"
        }
    }
    Messenger.process_new_tenant(mock_self,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mock_self._Messenger__bootstrap_tenants.assert_called_once_with("sample-subject",
        "sample-tenant",
        "sample-subject-mode")
    mock_self.emit.assert_called_once_with(
        "sample-tenancy-subject",
        "sample-management-tenant",
        "new-tenant",
        "sample-tenant")


    # Invalid payload
    reset_scenario()
    Messenger.process_new_tenant(mock_self,
        "sample-tenant",
        'wrong-payload')
    mock_self._Messenger__bootstrap_tenants.assert_not_called()
    mock_self.emit.assert_not_called()


    # Missing tenant in data
    reset_scenario()
    Messenger.process_new_tenant(mock_self,
        "sample-tenant",
        '{"key" : "not-expected"}')
    mock_self._Messenger_bootstrap_tenants.assert_not_called()
    mock_self.emit.assert_not_called()

    # Tenant already bootstrapped
    reset_scenario()
    mock_self.tenants = ["sample-tenant"]
    Messenger.process_new_tenant(mock_self,
        "sample-tenant",
        '{"tenant": "sample-tenant"}')
    mock_self._Messenger__bootstrap_tenants.assert_not_called()
    mock_self.emit.assert_not_called()

def test_messenger_emit():

    mock_self = Mock(
        event_callbacks={}
    )

    mock_callback = Mock()

    def reset_scenario():
        mock_self.event_callbacks = {
            "registered-subject": {
                "registered-event": [mock_callback]
            }
        }

    # No registered subject nor event
    reset_scenario()
    Messenger.emit(mock_self,
        "sample-subject",
        "sample-tenant",
        "sample-event",
        "sample-data")
    mock_callback.assert_not_called()


    # Registered subject, but not event
    reset_scenario()
    Messenger.emit(mock_self,
        "registered-subject",
        "sample-tenant",
        "sample-event",
        "sample-data")
    mock_callback.assert_not_called()

    # Not registered subject, but registered event
    reset_scenario()
    Messenger.emit(mock_self,
        "sample-subject",
        "sample-tenant",
        "registered-event",
        "sample-data")
    mock_callback.assert_not_called()

    # Registered subject and event
    reset_scenario()
    Messenger.emit(mock_self,
        "registered-subject",
        "sample-tenant",
        "registered-event",
        "sample-data")
    mock_callback.assert_called_once_with("sample-tenant", "sample-data")


def test_messenger_on():

    mock_self = Mock(
        event_callbacks={},
        create_channel=Mock(),
        subjects={},
        global_subjects={}
    )

    def reset_scenario():
        mock_self.event_callbacks = {}
        mock_self.subjects = {}
        mock_self.global_subjects = {}
        mock_self.create_channel.reset_mock()

    # No registered subjects
    Messenger.on(mock_self, "sample-subject", "sample-event", "callback-dummy")
    assert "sample-subject" in mock_self.event_callbacks
    assert "sample-event" in mock_self.event_callbacks["sample-subject"]
    assert "callback-dummy" in mock_self.event_callbacks["sample-subject"]["sample-event"]
    mock_self.create_channel.assert_called_once_with("sample-subject")

    # Registered a local subject
    reset_scenario()
    mock_self.subjects = ["sample-subject"]
    Messenger.on(mock_self, "sample-subject", "sample-event", "callback-dummy")
    mock_self.create_channel.assert_not_called()

    # Registered a global subject
    reset_scenario()
    mock_self.global_subjects = ["sample-subject"]
    Messenger.on(mock_self, "sample-subject", "sample-event", "callback-dummy")
    mock_self.create_channel.assert_not_called()

def test_messenger_bootstrap_tenants():

    mock_self = Mock(
        topic_manager=Mock(
            get_topic=Mock()
        ),
        topics={},
        consumer=Mock(
            subscribe=Mock(),
            start=Mock(),
            topics=["sample-topic"]
        ),
        producer_topics={}
    )

    def reset_scenario():
        mock_self.topic_manager.get_topic.reset_mock()
        mock_self.topics = {}
        mock_self.consumer.subscribe.reset_mock()
        mock_self.consumer.start.reset_mock()
        mock_self.producer_topics = {}

    # Happy day, non-global
    mock_self.topic_manager.get_topic.return_value = "dummy-topic"
    Messenger._Messenger__bootstrap_tenants(mock_self, "sample-subject", "sample-tenant", "rw")
    mock_self.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    assert "dummy-topic" in mock_self.topics
    mock_self.consumer.subscribe.assert_called_once_with("dummy-topic", ANY)
    mock_self.consumer.start.assert_called_once()
    assert "sample-subject" in mock_self.producer_topics
    assert "sample-tenant" in mock_self.producer_topics["sample-subject"]

    # Topic manager returned none
    reset_scenario()
    mock_self.topic_manager.get_topic.return_value = None
    Messenger._Messenger__bootstrap_tenants(mock_self, "sample-subject", "sample-tenant", "rw")
    mock_self.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    assert "dummy-topic" not in mock_self.topics
    mock_self.consumer.subscribe.assert_not_called()
    mock_self.consumer.start.assert_not_called()
    assert "sample-subject" not in mock_self.producer_topics

    # Already have such topic
    reset_scenario()
    mock_self.topics = ["dummy-topic"]
    mock_self.topic_manager.get_topic.return_value = "dummy-topic"
    Messenger._Messenger__bootstrap_tenants(mock_self, "sample-subject", "sample-tenant", "rw")
    mock_self.topic_manager.get_topic.assert_called_once_with("sample-tenant", "sample-subject", False)
    mock_self.consumer.subscribe.assert_not_called()
    mock_self.consumer.start.assert_not_called()
    assert "sample-subject" not in mock_self.producer_topics

def test_messenger_process_kafka_messages():
    mock_self = Mock(
        emit=Mock(),
        topics={}
    )

    def reset_scenario():
        mock_self.emit.reset_mock()
        mock_self.topics={}

    # Happy day
    mock_self.topics = {
        "sample-topic": {
            "subject": "sample-subject",
            "tenant": "sample-tenant"
        }
    }
    Messenger._Messenger__process_kafka_messages(mock_self, "sample-topic", "sample-message")
    mock_self.emit.assert_called_once_with("sample-subject", "sample-tenant", "message", "sample-message")

    # No registered topic
    reset_scenario()
    Messenger._Messenger__process_kafka_messages(mock_self, "sample-topic", "sample-message")
    mock_self.emit.assert_not_called()

def test_messenger_publish():
    mock_self = Mock(
        producer_topics={},
        producer=Mock(
            produce=Mock()
        )
    )

    def reset_scenario():
        mock_self.producer_topics={}
        mock_self.producer.produce.reset_mock()

    # Happy day
    mock_self.producer_topics = {
        "sample-subject": {
            "sample-tenant": "sample-topic"
        }
    }
    Messenger.publish(mock_self, "sample-subject", "sample-tenant", "sample-message")
    mock_self.producer.produce.assert_called_once_with("sample-topic", "sample-message")

    # No registered subject
    reset_scenario()
    Messenger.publish(mock_self, "sample-subject", "sample-tenant", "sample-message")
    mock_self.producer.produce.assert_not_called()

    # No registered tenant
    mock_self.producer_topics = {
        "sample-subject": {
            "other-tenant": "sample-topic"
        }
    }
    Messenger.publish(mock_self, "sample-subject", "sample-tenant", "sample-message")
    mock_self.producer.produce.assert_not_called()



def test_messenger_request_device():
    mock_self = Mock(
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
        mock_self.emit.reset_mock()

    patchReqGet = patch("requests.get", return_value=mockResponse)
    patchSleep = patch("time.sleep")

    with patchSleep, patchReqGet as mockReqRequest:
        Messenger.request_device(mock_self, "sample-tenant")
        mockReqRequest.assert_called_with("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })

        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-1\"}")
        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-2\"}")

    reset_scenario()

    mockResponse.status_code = 301
    patchReqGet = patch("requests.get", return_value=mockResponse)

    with patchSleep, patchReqGet as mockReqRequest:
        Messenger.request_device(mock_self, "sample-tenant")
        mockReqRequest.assert_called_with("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })

        mock_self.emit.assert_not_called()

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
        Messenger.request_device(mock_self, "sample-tenant")
        mockReqRequest.assert_any_call("http://sample-url/device", headers={
            'authorization': "Bearer super-secret-token"
        })
        mockReqRequest.assert_any_call("http://sample-url/device?page_num=199", headers={
            'authorization': "Bearer super-secret-token"
        })
        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-1\"}")
        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-2\"}")
        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-3\"}")
        mock_self.emit.assert_any_call("devices-subject", "sample-tenant", "message", "{\"event\": \"create\", \"data\": \"device-4\"}")


    def assert_exceptions(patchSleep, patchReqGet):
        with patchSleep, patchReqGet as mockReqRequest:
            Messenger.request_device(mock_self, "sample-tenant")
            mockReqRequest.assert_any_call("http://sample-url/device", headers={
                'authorization': "Bearer super-secret-token"
            })
            mock_self.emit.assert_not_called()

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
    mock_self = Mock(
        tenants=["tenant-1", "tenant-2"],
        request_device=Mock()
    )

    Messenger.generate_device_create_event_for_active_devices(mock_self)
    mock_self.request_device.assert_any_call("tenant-1")
    mock_self.request_device.assert_any_call("tenant-2")


def test_messenger_shutdown():
    mock_self = Mock(
        consumer=Mock(stop=Mock())
    )

    Messenger.shutdown(mock_self)
    mock_self.consumer.stop.assert_called_once()


def test_messenger_create_channel():
    mock_self = Mock(
        tenants=["sample-tenant-1", "sample-tenant-2"],
        subjects={},
        global_subjects={},
        __bootstrap_tenants = Mock(),
        config=Mock(
            dojot={
                "management": {
                    "tenant": "sample-mgmt-tenant"
                }
            }
        )
    )

    Messenger.create_channel(mock_self, "sample-subject", "r", False)
    mock_self.__bootstrap_tenants.expect_called_with("sample-subject", "sample-tenant-1", "r", False)
    mock_self.__bootstrap_tenants.expect_called_with("sample-subject", "sample-tenant-2", "r", False)
    assert "sample-subject" in mock_self.subjects

    mock_self.__bootstrap_tenants.reset_mock()
    Messenger.create_channel(mock_self, "sample-subject", "r", True)
    mock_self.__bootstrap_tenants.expect_called_with("sample-subject", "sample-mgmt-tenant", "r", True)
    assert "sample-subject" in mock_self.global_subjects

