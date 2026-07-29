{{ config(materialized='view', schema='silver') }}

SELECT
    id,
    raw_data->>'firstName'  AS first_name,
    raw_data->>'lastName'   AS last_name,
    raw_data->>'email'      AS email,
    raw_data->>'gender'     AS gender,
    raw_data->>'age'        AS age,
    loaded_at
FROM {{ source('bronze', 'escuela_users') }}
WHERE raw_data IS NOT NULL
  AND raw_data->>'email' IS NOT NULL
