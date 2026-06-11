import pytest
from unittest.mock import MagicMock, patch
from src.load.postgres_loader import load_to_bronze


@patch("src.load.postgres_loader.get_connection")
def test_load_to_bronze_success(mock_get_connection):
    mock_connection = MagicMock()
    mock_get_connection.return_value.__enter__.return_value = mock_connection

    test_data = {"key": "value"}
    test_table = "bronze.dummy_products"

    load_to_bronze(raw_data=test_data, table_name=test_table)

    assert mock_connection.execute.call_count == 1
    assert mock_connection.commit.call_count == 1


@patch("src.load.postgres_loader.get_connection")
def test_load_to_bronze_exception(mock_get_connection):
    mock_connection = MagicMock()
    mock_get_connection.return_value.__enter__.return_value = mock_connection
    mock_connection.execute.side_effect = Exception("Database Error")

    with pytest.raises(Exception):
        load_to_bronze(raw_data={"key": "value"}, table_name="bronze.dummy_products")