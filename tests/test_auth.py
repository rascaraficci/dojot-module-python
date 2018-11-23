import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock
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

def test_get_tenants_ok():
    config = Mock(
        auth={
            "url": "http://sample-url"
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
    patchResponse = patch("requests.Response.json", return_value = {"tenants": "123"})
    patchReqGet = patch("requests.get", return_value=requests.Response)
    with patchResponse, patchReqGet as mockReqGet:
        tenants = Auth.get_tenants(mockSelf)
        mockReqGet.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer 123"
        })
        assert tenants == "123"
    
