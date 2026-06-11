import pytest
import requests_mock
from extract.api_client import extract_from_api
from requests.exceptions import HTTPError

def test_extract_from_api_success():
    test_url = "https://fake-api.com"
    mock_response = [{"id": 1, "title": "Test Product"}]

    with requests_mock.Mocker() as mock:
        mock.get(test_url, json=mock_response, status_code=200)
        result = extract_from_api(test_url)
        assert result == mock_response


def test_extract_from_api_failure():
    test_url = "https://broken-api.com"

    with requests_mock.Mocker() as mock:
        mock.get(test_url, status_code=500)

        with pytest.raises(HTTPError):
            extract_from_api(test_url)

