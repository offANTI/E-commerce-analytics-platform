import pytest
import requests

from extract.api_client import APIClient


def test_get_success(monkeypatch):
    client = APIClient(
        base_url="https://example.com",
        timeout=5,
    )

    class MockResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"id": 1, "name": "Test"}

    def mock_get(*args, **kwargs):
        assert args[0] == "https://example.com/products"
        assert kwargs["params"] == {"limit": 10}
        assert kwargs["timeout"] == 5

        return MockResponse()

    monkeypatch.setattr(client.session, "get", mock_get)

    result = client.get(
        "products",
        params={"limit": 10},
    )

    assert result == {"id": 1, "name": "Test"}


def test_get_timeout(monkeypatch):
    client = APIClient(base_url="https://example.com")

    def mock_get(*args, **kwargs):
        raise requests.exceptions.Timeout()

    monkeypatch.setattr(client.session, "get", mock_get)

    with pytest.raises(requests.exceptions.Timeout):
        client.get("products")


def test_get_http_error(monkeypatch):
    client = APIClient(base_url="https://example.com")

    class MockResponse:
        def raise_for_status(self):
            raise requests.exceptions.HTTPError("500")

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: MockResponse(),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        client.get("products")


def test_get_request_error(monkeypatch):
    client = APIClient(base_url="https://example.com")

    def mock_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError("connection failed")

    monkeypatch.setattr(client.session, "get", mock_get)

    with pytest.raises(requests.exceptions.ConnectionError):
        client.get("products")


def test_get_invalid_json(monkeypatch):
    client = APIClient(base_url="https://example.com")

    class MockResponse:
        def raise_for_status(self):
            pass

        def json(self):
            raise ValueError("invalid json")

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: MockResponse(),
    )

    with pytest.raises(ValueError):
        client.get("products")