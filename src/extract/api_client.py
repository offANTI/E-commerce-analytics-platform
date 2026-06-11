from typing import Any

import requests

from utils.logger import get_project_logger


logger = get_project_logger(__name__)


def extract_from_api(url: str, timeout: int = 10) -> dict[str, Any] | list[dict[str, Any]]:
    logger.info("Downloading data from API: %s", url)

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        data = response.json()

        logger.info("Data successfully extracted from API: %s", url)
        return data

    except requests.RequestException:
        logger.exception("API request failed: %s", url)
        raise

    except ValueError:
        logger.exception("Failed to parse JSON response from API: %s", url)
        raise