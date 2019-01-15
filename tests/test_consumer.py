from kafka import KafkaConsumer
from kafka.errors import IllegalStateError
from unittest.mock import patch, MagicMock, Mock
from dojot.module.kafka import Consumer
import threading

def test_consumer_init_ok():
    with patch.object(KafkaConsumer, "__init__", return_value=None) as mockKafkaConsumer:
        config = Mock(kafka={
            "consumer": {
                "bootstrap_servers": ["k1","k2","k3"],
                "group_id": "sample-group-id"
            },
            "dojot": {
                "poll_timeout": 1,
                "subscription_holdoff": 2.5
            }
        })
        consumer = Consumer(config, "sample-name")
        assert consumer is not None
        assert consumer.name == "sample-name"
        assert consumer.topics == []
        assert consumer.poll_timeout == 1
        assert consumer.callback is None
        assert consumer.consumer is not None
        mockKafkaConsumer.assert_called_with(
            bootstrap_servers=["k1", "k2", "k3"], group_id="sample-group-id")

def test_reset_subscriptions():
    mock_self = Mock(topics=["sample-topic"])
    Consumer._reset_subscriptions(mock_self)
    mock_self.consumer.subscribe.assert_called_with(topics=["sample-topic"])

    mock_self.consumer.subscribe = Mock(side_effect=IllegalStateError)
    Consumer._reset_subscriptions(mock_self)

    mock_self.consumer.subscribe = Mock(side_effect=AssertionError())
    Consumer._reset_subscriptions(mock_self)

    mock_self.consumer.subscribe = Mock(side_effect=TypeError())
    Consumer._reset_subscriptions(mock_self)

def test_subscribe():
    mock_self = Mock(
        topics=["sample-topic"],
        should_reset_consumer=Mock(set=Mock()),
        topic_lock=threading.RLock()
    )

    Consumer.subscribe(mock_self, "extra-topic", "callback")
    assert mock_self.topics == [ "sample-topic", "extra-topic"]
    mock_self.should_reset_consumer.expect_called_once()


def test_consumer_run_ok():
    msg_a = Mock(topic="topic-a", value="value-a")
    msg_b = Mock(topic="topic-b", value="value-b")
    msg_c = Mock(topic="topic-c", value="value-c")
    mock_self = Mock(
        consumer=Mock(
            poll=Mock(return_value=
                {
                    "topic-a": [msg_a],
                    "topic-b": [msg_b],
                    "topic-c": [msg_c]
                }),
            callback=Mock()
        ),
        should_stop=Mock(is_set=Mock(side_effect=[False, True])),
        should_reset_consumer=Mock(is_set=Mock(side_effect=[False])),
        topic_lock=threading.RLock(),
        _reset_subscriptions=Mock(),
        subscription_holdoff=1,
        topics=["topic-x"]

    )
    def reset_scenario():
        mock_self.should_stop.is_set.reset_mock()
        mock_self.should_reset_consumer.is_set.reset_mock()
        mock_self._reset_subscriptions.reset_mock()

    reset_scenario()
    Consumer.run(mock_self)
    mock_self.callback.assert_any_call("topic-c", "value-c")
    mock_self.callback.assert_any_call("topic-b", "value-b")
    mock_self.callback.assert_any_call("topic-a", "value-a")

    reset_scenario()
    mock_self.should_stop.is_set.side_effect = [False, False, False, True]
    mock_self.should_reset_consumer.is_set.side_effect=[False, True, False]
    Consumer.run(mock_self)
    mock_self.callback.assert_any_call("topic-c", "value-c")
    mock_self.callback.assert_any_call("topic-b", "value-b")
    mock_self.callback.assert_any_call("topic-a", "value-a")
    mock_self._reset_subscriptions.assert_called_once()
    mock_self.should_reset_consumer.clear.assert_called_once()



def test_consumer_stop():
    mock_self = Mock(
        should_stop=Mock(set=Mock())
    )

    Consumer.stop(mock_self)
    mock_self.should_stop.set.assert_called_once()
