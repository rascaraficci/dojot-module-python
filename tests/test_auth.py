import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock, DEFAULT
from dojot.module import Auth, Config, HttpRequester

def test_get_management_token_ok():
    config = Mock(
        dojot={
            "management": {
                "user" : "sample-user",
                "tenant" : "sample-tenant"
            }
        }
    )
    mock_self = Mock(config=config)
    token = Auth.get_management_token(mock_self)
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
    mock_self = Mock(config=config)
    token = Auth.get_access_token(mock_self, "non-management-tenant")
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
    mock_self = Mock(
        get_management_token=Mock(return_value="123"),
        config=config
    )

    patch_http_perform = patch("dojot.module.HttpRequester.do_it", return_value={"tenants": "admin"})
    with patch_http_perform as mock_http_perform:
        tenants = Auth.get_tenants(mock_self)
        mock_http_perform.assert_called_with("http://sample-url/admin/tenants", "123", 3, 1)
        assert tenants == "admin"
    patch_http_perform = patch("dojot.module.HttpRequester.do_it", return_value=None)
    with patch_http_perform as mock_http_perform:
        tenants = Auth.get_tenants(mock_self)
        mock_http_perform.assert_called_with("http://sample-url/admin/tenants", "123", 3, 1)
        assert tenants is None
