import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock, create_autospec
from dojot.module.kafka import TopicManager


@patch("dojot.module.Auth")
def test_topic_manager_init(patchAuth):
    config = Mock(data_broker={
        "url": "http://sample-url"
    })
    topicManager = TopicManager(config)
    assert(topicManager.topics == {})
    assert(topicManager.tenants == [])
    assert(topicManager.broker == "http://sample-url")
    assert(topicManager.auth is not None)


def test_get_key():
    ret = TopicManager.get_key("sample-tenant", "sample-subject")
    assert(ret == "sample-tenant:sample-subject")

def test_get_topic_ok():
    mockAuth = Mock(get_management_token=Mock(return_value="sample-token"))
    patchResponse = patch("requests.Response.json", return_value={"topic": "sample-topic"})
    patchReqGet = patch("requests.get", return_value=requests.Response)
    with patchResponse, patchReqGet as mockReqGet:
        mockSelf = Mock(topics={}, broker="http://sample-broker", auth=mockAuth)
        ret = TopicManager.get_topic(mockSelf, "sample-tenant", "sample-subject")
        assert(ret == "sample-topic")
        mockReqGet.assert_called_once_with("http://sample-broker/topic/sample-subject",
                                headers={"authorization": "Bearer sample-token"})


def test_get_topic_global():
    mockAuth = Mock(get_management_token=Mock(return_value="sample-token-global"))
    patchResponse = patch("requests.Response.json", return_value={"topic": "sample-topic-global"})
    patchReqGet = patch("requests.get", return_value=requests.Response)
    with patchResponse, patchReqGet as mockReqGet:
        mockSelf = Mock(topics={}, broker="http://sample-broker", auth=mockAuth)
        ret = TopicManager.get_topic(mockSelf, "sample-tenant", "sample-subject-global", "global")
        assert(ret == "sample-topic-global")
        mockReqGet.assert_called_once_with("http://sample-broker/topic/sample-subject-global?global=true",
                                headers={"authorization": "Bearer sample-token-global"})

def test_get_topic_value_error():
    mockAuth = Mock(get_management_token=Mock(return_value="sample-token-error"))
    patchResponseJson = patch("requests.Response.json", side_effect=ValueError("nope"))
    patchReqGet = patch("requests.get", return_value=requests.Response)
    with patchResponseJson, patchReqGet as mockReqGet:
        mockSelf = Mock(topics={}, broker="http://sample-broker", auth=mockAuth)
        ret = TopicManager.get_topic(mockSelf, "sample-tenant", "sample-subject-error")
        assert(ret is None)
        mockReqGet.assert_called_once_with("http://sample-broker/topic/sample-subject-error",
                                headers={"authorization": "Bearer sample-token-error"})
 

def test_get_topic_with_topic():
    mockSelf = Mock(get_key=TopicManager.get_key, topics={"sample-tenant:sample-subject": "sample-stored-value"})
    ret = TopicManager.get_topic(mockSelf, "sample-tenant", "sample-subject")
    assert(ret == "sample-stored-value")
