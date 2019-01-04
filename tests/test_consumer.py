from kafka import KafkaConsumer
from kafka.errors import IllegalStateError
from unittest.mock import patch, MagicMock, Mock
from dojot.module.kafka import Consumer

def test_consumer_init_ok():
    with patch.object(KafkaConsumer, "__init__", return_value=None) as mockKafkaConsumer:
        config = Mock(kafka={
            "consumer": {
                "metadata.broker.list": "k1,k2,k3",
                "poll_timeout": 1
            }
        })
        consumer = Consumer("sample-group-id", config, "sample-name")
        assert consumer is not None
        assert consumer.topics == []
        assert consumer.broker == ["k1", "k2", "k3"]
        assert consumer.group_id == "sample-group-id"
        assert consumer.callback is None
        assert consumer.consumer is not None
        mockKafkaConsumer.assert_called_with(
            bootstrap_servers=["k1", "k2", "k3"], group_id="sample-group-id")

def test_consumer_subscribe_ok():
    mockSelf = Mock(topics=[],
            consumer = Mock(subscribe=MagicMock()),
            callback="")
    Consumer.subscribe(mockSelf, "sample-topic", "callback")
    assert mockSelf.topics == ["sample-topic"]
    assert mockSelf.callback == "callback"
    mockSelf.consumer.subscribe.assert_not_called()

    mockSelf.consumer.subscribe = Mock(side_effect=IllegalStateError)
    Consumer.subscribe(mockSelf, "sample-topic-illegal", "callback-illegal")

    mockSelf.consumer.subscribe = Mock(side_effect=AssertionError())
    Consumer.subscribe(mockSelf, "sample-topic-assertion", "callback-assertion")

    mockSelf.consumer.subscribe = Mock(side_effect=TypeError())
    Consumer.subscribe(mockSelf, "sample-topic-type-error", "callback-type-error")

def test_consumer_run_ok():
    msgA = Mock(topic="topic-a", value="value-a")
    msgB = Mock(topic="topic-b", value="value-b")
    msgC = Mock(topic="topic-c", value="value-c")
    mockSelf = Mock(
        consumer=Mock(
            poll=Mock(return_value=
                {
                    "topic-a": [msgA],
                    "topic-b": [msgB],
                    "topic-c": [msgC]
                }),
            callback=Mock()
        ),
        should_stop=Mock(is_set=Mock(side_effect=[False, True])),
        should_reset_consumer=Mock(is_set=Mock(side_effect=[False])),
        _reset_subscriptions=Mock()
    )
    def reset_scenario():
        mockSelf.should_stop.is_set.reset_mock()
        mockSelf.should_reset_consumer.is_set.reset_mock()
        mockSelf._reset_subscriptions.reset_mock()

    reset_scenario()
    Consumer.run(mockSelf)
    mockSelf.callback.assert_any_call("topic-c", "value-c")
    mockSelf.callback.assert_any_call("topic-b", "value-b")
    mockSelf.callback.assert_any_call("topic-a", "value-a")

    reset_scenario()
    mockSelf.should_stop.is_set.side_effect = [False, False, False, True]
    mockSelf.should_reset_consumer.is_set.side_effect=[False, True, False]
    Consumer.run(mockSelf)
    mockSelf.callback.assert_any_call("topic-c", "value-c")
    mockSelf.callback.assert_any_call("topic-b", "value-b")
    mockSelf.callback.assert_any_call("topic-a", "value-a")
    mockSelf._reset_subscriptions.assert_called_once()
    mockSelf.should_reset_consumer.clear.assert_called_once()


def test_consumer_reset_subscriptions():
    mockSelf = Mock(
        consumer=Mock(
            subscribe=Mock(),
            subscription=Mock(return_value="nope")
        ),
        topics=["sample-1"],
        topic_lock=Mock(acquire=Mock(), release=Mock())
    )
    def reset_scenario():
        mockSelf.consumer.subscribe.reset_mock()
        mockSelf.topic_lock.acquire.reset_mock()
        mockSelf.topic_lock.release.reset_mock()


    def assert_function_called():
        mockSelf.topic_lock.acquire.assert_called_once()
        mockSelf.topic_lock.release.assert_called_once()
        mockSelf.consumer.subscribe.assert_called_once_with(topics=["sample-1"])

    reset_scenario()
    Consumer._reset_subscriptions(mockSelf)
    assert_function_called()

    reset_scenario()
    mockSelf.consumer.subscribe.side_effect=[IllegalStateError]
    Consumer._reset_subscriptions(mockSelf)
    assert_function_called()

    reset_scenario()
    mockSelf.consumer.subscribe.side_effect=AssertionError
    Consumer._reset_subscriptions(mockSelf)
    assert_function_called()

    reset_scenario()
    mockSelf.consumer.subscribe.side_effect=TypeError
    Consumer._reset_subscriptions(mockSelf)
    assert_function_called()


def test_consumer_stop():
    mockSelf = Mock(
        should_stop=Mock(set=Mock())
    )

    Consumer.stop(mockSelf)
    mockSelf.should_stop.set.assert_called_once()

