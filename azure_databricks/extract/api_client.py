from typing import Any

import requests


class APIClient:
    def __init__(self, base_url: str, timeout: int = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        print(f"Extracting data from API: {url}")

        try:
            response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            print(f"Successfully extracted data from API: {url}")
            return data
        except requests.exceptions.Timeout:
            print(f"API timeout: {url}")
            raise
        except requests.exceptions.HTTPError:
            print(f"API returned HTTP error: {url}")
            raise
        except requests.exceptions.RequestException:
            print(f"API request failed: {url}")
            raise
        except ValueError:
            print(f"Invalid JSON response: {url}")
            raise