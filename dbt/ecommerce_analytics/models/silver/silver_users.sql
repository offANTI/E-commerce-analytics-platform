{{ config(materialized='view', schema='silver') }}

WITH ranked AS (
    SELECT
        (item->>'id')::INT AS user_id,
        (item->>'email')::VARCHAR(255) AS email,
        (item->>'name')::VARCHAR(255) AS name,
        (item->>'role')::VARCHAR(50) AS role,
        (item->>'avatar')::TEXT AS avatar_url,
        ROW_NUMBER() OVER (PARTITION BY item->>'id' ORDER BY loaded_at DESC) AS rn
    FROM {{ source('bronze', 'escuela_users') }},
    LATERAL jsonb_array_elements(raw_data) AS item
)
SELECT user_id, email, name, role, avatar_url
FROM ranked
WHERE rn = 1