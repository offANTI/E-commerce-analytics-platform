from unittest.mock import MagicMock, patch

import pytest

from src.transform.orders import generate_mock_orders


@patch("src.transform.orders.engine")
def test_generate_mock_orders_empty_db(mock_engine):
    mock_connection = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_connection  # begin, не connect

    mock_res_users = MagicMock()
    mock_res_users.fetchall.return_value = []
    mock_connection.execute.return_value = mock_res_users

    with pytest.raises(ValueError, match="No users found"):
        generate_mock_orders()
