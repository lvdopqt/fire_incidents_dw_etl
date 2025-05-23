{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['district_key'], 'unique': True, 'type': 'btree'},
            {'columns': ['district_name'], 'unique': True, 'type': 'btree'}
        ]
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['supervisor_district']) }} AS district_key,
    supervisor_district AS district_name
FROM (
    SELECT DISTINCT
        supervisor_district
    FROM {{ ref('base_fire_incidents') }}
    WHERE supervisor_district IS NOT NULL
) AS unique_districts
ORDER BY supervisor_district