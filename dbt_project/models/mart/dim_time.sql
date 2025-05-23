{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['time_key'], 'unique': True, 'type': 'btree'},
            {'columns': ['date_day'], 'unique': True, 'type': 'btree'},
            {'columns': ['year'], 'type': 'btree'},
            {'columns': ['quarter'], 'type': 'btree'}
        ]
    )
}}

SELECT
    TO_CHAR(incident_date, 'YYYYMMDD')::INT AS time_key,
    incident_date AS date_day,
    EXTRACT(YEAR FROM incident_date)::INT AS year,
    EXTRACT(MONTH FROM incident_date)::INT AS month,
    EXTRACT(DAY FROM incident_date)::INT AS day,
    EXTRACT(DOW FROM incident_date)::INT AS day_of_week,
    EXTRACT(DOY FROM incident_date)::INT AS day_of_year,
    EXTRACT(WEEK FROM incident_date)::INT AS week_of_year,
    EXTRACT(QUARTER FROM incident_date)::INT AS quarter,
    TRUE AS is_weekday,
    TO_CHAR(incident_date, 'Day') AS day_name

FROM (
    SELECT DISTINCT incident_date
    FROM {{ ref('base_fire_incidents') }}
    WHERE incident_date IS NOT NULL
) AS unique_incident_dates
ORDER BY incident_date
