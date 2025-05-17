SELECT
    incident_number,
    exposure_number,
    COUNT(*) AS num_occurrences

FROM {{ ref('fct_fire_incidents') }}

GROUP BY
    incident_number,
    exposure_number

HAVING COUNT(*) > 1
