import pytest
import uuid
import json
from unittest.mock import Mock, patch, ANY, DEFAULT
from dojot.module import Messenger, Auth, HttpRequester
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests

def test_http_request():
    mock_response = Mock(
        json=Mock(return_value={"attr": "x"}),
        text="there was an error, but I will not tell you.",
        status_code=200
    )
    patch_req_get = patch("requests.get", return_value=mock_response)
    with patch_req_get as mock_req_get:
        response = HttpRequester.do_it("http://sample-url/admin/tenants", "token", 1, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token"
        })
        assert response == {"attr": "x"}

    mock_response.status_code=404
    patch_req_get = patch("requests.get", return_value=mock_response)
    with patch_req_get as mock_req_get:
        tenants = HttpRequester.do_it("http://sample-url/admin/tenants", "token-123-answer", 1, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-answer"
        })
        assert tenants is None

    mock_response.status_code=200
    mock_response.json.side_effect=ValueError
    patch_req_get = patch("requests.get", return_value=mock_response)
    with patch_req_get as mock_req_get:
        tenants = HttpRequester.do_it("http://sample-url/admin/tenants", "token-123-value-error", 1, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-value-error"
        })
        assert tenants is None


    mock_response.json.side_effect=None
    mock_response.json.return_value = {"tenants": "123-connection"}
    patch_req_get = patch("requests.get", return_value=mock_response, side_effect=[requests.exceptions.ConnectionError, DEFAULT])
    with patch_req_get as mock_req_get:
        tenants = HttpRequester.do_it("http://sample-url/admin/tenants", "token-123-connection", 2, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-connection"
        })
        assert tenants == {"tenants": "123-connection"}

    mock_response.json.return_value = {"tenants": "123-timeout"}
    patch_req_get = patch("requests.get", return_value=mock_response, side_effect=[requests.exceptions.Timeout, DEFAULT])
    with patch_req_get as mock_req_get:
        tenants = HttpRequester.do_it("http://sample-url/admin/tenants", "token-123-timeout", 2, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-timeout"
        })
        assert tenants == {"tenants" : "123-timeout"}


    mock_response.json.return_value = {"tenants": "123-redirects"}
    patch_req_get = patch("requests.get", return_value=mock_response, side_effect=[requests.exceptions.TooManyRedirects, DEFAULT])
    with patch_req_get as mock_req_get:
        tenants = HttpRequester.do_it("http://sample-url/admin/tenants", "token-123-redirects", 1, 1)
        mock_req_get.assert_called_with("http://sample-url/admin/tenants", headers = {
            "authorization": "Bearer token-123-redirects"
        })
        assert tenants is None
