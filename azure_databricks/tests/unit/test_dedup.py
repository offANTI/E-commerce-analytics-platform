from transform.dedup import deduplicate_by_key


def test_deduplicate_keeps_latest_record(spark):
    """Keep the latest record by loaded_at."""

    rows = [
        {
            "user_id": 1,
            "value": "old",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 1,
        },
        {
            "user_id": 1,
            "value": "new",
            "loaded_at": "2026-01-02T00:00:00Z",
            "ingestion_id": 2,
        },
        {
            "user_id": 2,
            "value": "user2",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 3,
        },
    ]

    df = spark.createDataFrame(rows)

    result = deduplicate_by_key(
        df,
        key_column="user_id",
    )

    result_rows = {
        row["user_id"]: row["value"]
        for row in result.collect()
    }

    assert result.count() == 2
    assert result_rows[1] == "new"
    assert result_rows[2] == "user2"


def test_deduplicate_removes_duplicates(spark):
    """Result should contain only one row per key."""

    rows = [
        {
            "user_id": 1,
            "value": "a",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 1,
        },
        {
            "user_id": 1,
            "value": "b",
            "loaded_at": "2026-01-02T00:00:00Z",
            "ingestion_id": 2,
        },
        {
            "user_id": 2,
            "value": "c",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 3,
        },
    ]

    df = spark.createDataFrame(rows)

    result = deduplicate_by_key(
        df,
        key_column="user_id",
    )

    assert result.count() == 2
    assert result.select("user_id").distinct().count() == 2


def test_deduplicate_uses_ingestion_id_as_tiebreaker(spark):
    """Use ingestion_id when loaded_at values are equal."""

    rows = [
        {
            "user_id": 1,
            "value": "first",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 10,
        },
        {
            "user_id": 1,
            "value": "second",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 20,
        },
    ]

    df = spark.createDataFrame(rows)

    result = deduplicate_by_key(
        df,
        key_column="user_id",
    )

    row = result.first()

    assert row["value"] == "second"


def test_deduplicate_preserves_unique_rows(spark):
    """Rows with unique keys should remain unchanged."""

    rows = [
        {
            "user_id": 1,
            "value": "user1",
            "loaded_at": "2026-01-01T00:00:00Z",
            "ingestion_id": 1,
        },
        {
            "user_id": 2,
            "value": "user2",
            "loaded_at": "2026-01-02T00:00:00Z",
            "ingestion_id": 2,
        },
        {
            "user_id": 3,
            "value": "user3",
            "loaded_at": "2026-01-03T00:00:00Z",
            "ingestion_id": 3,
        },
    ]

    df = spark.createDataFrame(rows)

    result = deduplicate_by_key(
        df,
        key_column="user_id",
    )

    assert result.count() == 3

    result_ids = {
        row["user_id"]
        for row in result.collect()
    }

    assert result_ids == {1, 2, 3}