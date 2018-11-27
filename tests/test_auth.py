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

    def reset_scenario():
        mockSelf.get_management_token.reset_mock()


    patchResponse = patch("requests.Response.json", return_value = {"tenants": "123"})
    patchReqGet = patch("requests.get", return_value=requests.Response)
    with patchResponse, patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer 123"
        })
        assert tenants == "123"
    
    reset_scenario()
    patchResponse = patch("requests.Response.json", return_value = {"tenants": "123-connection"})
    patchReqGet = patch("requests.get", return_value=requests.Response, side_effect=[requests.exceptions.ConnectionError, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-connection"
    with patchResponse, patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-connection"
        })
        assert tenants == "123-connection"
    
    reset_scenario()
    patchResponse = patch("requests.Response.json", return_value = {"tenants": "123-timeout"})
    patchReqGet = patch("requests.get", return_value=requests.Response, side_effect=[requests.exceptions.Timeout, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-timeout"
    with patchResponse, patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-timeout"
        })
        assert tenants == "123-timeout"
    

    reset_scenario()
    patchResponse = patch("requests.Response.json", return_value = {"tenants": "123-redirects"})
    patchReqGet = patch("requests.get", return_value=requests.Response, side_effect=[requests.exceptions.TooManyRedirects, DEFAULT])
    mockSelf.get_management_token.return_value = "token-123-redirects"
    with patchResponse, patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-redirects"
        })
        assert tenants is None
