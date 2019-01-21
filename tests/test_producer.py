from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from unittest.mock import patch, MagicMock, Mock
from dojot.module.kafka import Producer

def test_producer_init_ok():
    config = Mock(kafka = {
        "producer": {
            "bootstrap_servers": ["k1","k2","k3"]
        }
    })

    producer = Producer(config)
    assert producer.broker == ["k1", "k2", "k3"]
    assert producer.producer is None


def test_producer_init():
    with patch.object(KafkaProducer, "__init__", return_value=None) as KafkaProducerMock:
        mock_self = Mock(broker="k1")
        Producer.init(mock_self)
        KafkaProducerMock.assert_called_once()

def test_producer_produce():
    mock_self = Mock(producer=Mock(send=Mock(), flush=Mock()))
    Producer.produce(mock_self, "sample-topic", "sample-message")
    mock_self.producer.send.assert_called_with("sample-topic", "sample-message")
    mock_self.producer.flush.assert_called_once()

    mock_self = Mock(producer=Mock(send=Mock(side_effect=KafkaTimeoutError, flush=Mock())))
    Producer.produce(mock_self, "sample-timeout-send", "sample-timeout-send")

    mock_self = Mock(producer=Mock(send=Mock(), flush=Mock(side_effect=KafkaTimeoutError)))
    Producer.produce(mock_self, "sample-timeout-flush", "sample-timeout-flush")
