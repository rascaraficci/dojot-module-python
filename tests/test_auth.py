import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock, DEFAULT
from dojot.module import Auth, Config

def test_get_management_token_ok():
    config = Mock(
        dojot={
            "management": { 
                "user" : "sample-user", 
                "tenant" : "sample-tenant"
            }
        }
    )
    mockSelf = Mock(config=config)
    token = Auth.get_management_token(mockSelf)
    assert token is not None

def test_get_access_token_ok():
    config = Mock(
        dojot={
            "management": { 
                "user" : "management-user", 
                "tenant" : "non-management-tenant"
            }
        }
    )
    mockSelf = Mock(config=config)
    token = Auth.get_access_token(mockSelf, "non-management-tenant")
    assert token is not None


def test_get_tenants():
    config = Mock(
        auth={
            "url": "http://sample-url",
            "timeout_sleep": 1,
            "connection_retries": 3
        },
        dojot={
            "management": {
                "user": "sample-user",
                "tenant": "sample-tenant"
            }
        }
    )
    mockSelf = Mock(
        get_management_token=Mock(return_value="123"),
        config=config
    )
    mockResponse = Mock(
        json=Mock(return_value={"tenants": "123"}), 
        status_code=200
    )

    def reset_scenario():
        mockSelf.get_management_token.reset_mock()
        mockResponse.json.reset_mock()
        mockResponse.json.side_effect = None
        mockResponse.status_code=200

    patchReqGet = patch("requests.get", return_value=mockResponse)
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer 123"
        })
        assert tenants == "123"
    
    reset_scenario()
    mockResponse.status_code=404
    mockResponse.json.return_value = {"tenants": "123-answer"}
    patchReqGet = patch("requests.get", return_value=mockResponse)
    mockSelf.get_management_token.return_value = "token-123-answer"
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-answer"
        })
        assert tenants is None
    
    reset_scenario()
    mockResponse.json.side_effect=ValueError
    mockResponse.json.return_value = {"tenants": "123-value-error"}
    patchReqGet = patch("requests.get", return_value=mockResponse)
    mockSelf.get_management_token.return_value = "token-123-value-error"
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-value-error"
        })
        assert tenants is None
    

    reset_scenario()
    mockResponse.json.return_value = {"tenants": "123-connection"}
    patchReqGet = patch("requests.get", return_value=mockResponse, side_effect=[requests.exceptions.ConnectionError, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-connection"
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-connection"
        })
        assert tenants == "123-connection"
    
    reset_scenario()
    mockResponse.json.return_value = {"tenants": "123-timeout"}
    patchReqGet = patch("requests.get", return_value=mockResponse, side_effect=[requests.exceptions.Timeout, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-timeout"
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-timeout"
        })
        assert tenants == "123-timeout"
    

    reset_scenario()
    mockResponse.json.return_value = {"tenants": "123-redirects"}
    patchReqGet = patch("requests.get", return_value=mockResponse, side_effect=[requests.exceptions.TooManyRedirects, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-redirects"
    with patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-redirects"
        })
        assert tenants is None
