import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock, create_autospec
from dojot.module.kafka import TopicManager
from dojot.module import HttpRequester


@patch("dojot.module.Auth")
def test_topic_manager_init(patchAuth):
    config = Mock(data_broker={
        "url": "http://sample-url",
        "connection_retries": 5,
        "timeout_sleep": 3
    })
    topic_manager = TopicManager(config)
    assert topic_manager.topics == {}
    assert topic_manager.tenants == []
    assert topic_manager.broker == "http://sample-url"
    assert topic_manager.auth is not None


def test_get_key():
    ret = TopicManager.get_key("sample-tenant", "sample-subject")
    assert ret == "sample-tenant:sample-subject"

def test_get_topic_ok():
    mock_auth = Mock(get_access_token=Mock(return_value="sample-token"))
    patch_http_requester = patch("dojot.module.HttpRequester.do_it", return_value={"topic": "sample-topic"})
    with patch_http_requester as mock_http_req:
        mock_self = Mock(topics={}, broker="http://sample-broker", auth=mock_auth, retry_counter=1, timeout_sleep=2)
        ret = TopicManager.get_topic(mock_self, "sample-tenant", "sample-subject")
        assert ret == "sample-topic"
        mock_http_req.assert_called_once_with("http://sample-broker/topic/sample-subject", "sample-token", 1, 2)

def test_get_topic_global():
    mock_auth = Mock(get_access_token=Mock(return_value="sample-token-global"))
    patch_http_requester = patch("dojot.module.HttpRequester.do_it", return_value={"topic": "sample-topic-global"})
    with patch_http_requester as mock_http_req:
        mock_self = Mock(topics={}, broker="http://sample-broker", auth=mock_auth, retry_counter=1, timeout_sleep=2)
        ret = TopicManager.get_topic(mock_self, "sample-tenant", "sample-subject-global", "global")
        assert ret == "sample-topic-global"
        mock_http_req.assert_called_once_with("http://sample-broker/topic/sample-subject-global?global=true", "sample-token-global", 1, 2)

def test_get_topic_value_error():
    mock_auth = Mock(get_access_token=Mock(return_value="sample-token"))
    patch_http_requester = patch("dojot.module.HttpRequester.do_it", return_value=None)
    with patch_http_requester as mock_http_req:
        mock_self = Mock(topics={}, broker="http://sample-broker", auth=mock_auth, retry_counter=1, timeout_sleep=2)
        ret = TopicManager.get_topic(mock_self, "sample-tenant", "sample-subject")
        assert ret is None
        mock_http_req.assert_called_once_with("http://sample-broker/topic/sample-subject", "sample-token", 1, 2)


def test_get_topic_with_topic():
    mock_self = Mock(get_key=TopicManager.get_key, topics={"sample-tenant:sample-subject": "sample-stored-value"})
    ret = TopicManager.get_topic(mock_self, "sample-tenant", "sample-subject")
    assert ret == "sample-stored-value"
