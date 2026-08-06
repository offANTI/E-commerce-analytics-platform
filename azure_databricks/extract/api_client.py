from typing import Any

import requests

from utils.logger import get_project_logger

logger = get_project_logger(__name__)


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
        logger.info("Extracting data from API: %s", url)

        try:
            response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully extracted data from API: %s", url)
            return data
        except requests.exceptions.Timeout:
            logger.exception("API timeout: %s", url)
            raise
        except requests.exceptions.HTTPError:
            logger.exception("API returned HTTP error: %s", url)
            raise
        except requests.exceptions.RequestException:
            logger.exception("API request failed: %s", url)
            raise
        except ValueError:
            logger.exception("Invalid JSON response: %s", url)
            raise