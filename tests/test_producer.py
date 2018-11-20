from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from unittest.mock import patch, MagicMock, Mock
from dojot.module.kafka import Producer

def test_producer_init_ok():
    config = Mock(kafka = {
        "producer": {
            "metadata.broker.list": "k1,k2,k3"
        }
    })

    producer = Producer(config)
    assert(producer.broker == ["k1", "k2", "k3"])
    assert(producer.producer is None)


def test_producer_init():
    with patch.object(KafkaProducer, "__init__", return_value=None):
        mockSelf = Mock(broker="k1")
        ret = Producer.init(mockSelf)
        assert(ret == 0)
    with patch.object(KafkaProducer, "__init__", side_effect=AssertionError):
        mockSelf = Mock(broker="k2")
        ret = Producer.init(mockSelf)
        assert(ret == -1)

def test_producer_produce():
    mockSelf = Mock(producer=Mock(send=Mock(), flush=Mock()))
    Producer.produce(mockSelf, "sample-topic", "sample-message")
    mockSelf.producer.send.assert_called_with("sample-topic", "sample-message")
    mockSelf.producer.flush.assert_called_once()
    
    mockSelf = Mock(producer=Mock(send=Mock(side_effect=KafkaTimeoutError, flush=Mock())))
    Producer.produce(mockSelf, "sample-timeout-send", "sample-timeout-send")
    
    mockSelf = Mock(producer=Mock(send=Mock(), flush=Mock(side_effect=KafkaTimeoutError)))
    Producer.produce(mockSelf, "sample-timeout-flush", "sample-timeout-flush")
    