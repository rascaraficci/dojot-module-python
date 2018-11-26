from kafka import KafkaConsumer
from kafka.errors import IllegalStateError
from unittest.mock import patch, MagicMock, Mock
from dojot.module.kafka import Consumer

def test_consumer_init_ok():
    with patch.object(KafkaConsumer, "__init__", return_value=None) as mockKafkaConsumer:
        config = Mock(kafka={
            "consumer": {
                "metadata.broker.list": "k1,k2,k3"
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
    mockSelf.consumer.subscribe.assert_called_with(topics=["sample-topic"])

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
    mockSelf = Mock(consumer=[msgA, msgB, msgC], callback=Mock())
    Consumer.run(mockSelf)
    mockSelf.callback.assert_any_call("topic-c", "value-c")
    mockSelf.callback.assert_any_call("topic-b", "value-b")
    mockSelf.callback.assert_any_call("topic-a", "value-a")
