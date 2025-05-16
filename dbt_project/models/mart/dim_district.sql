
SELECT
    {{ dbt_utils.generate_surrogate_key(['supervisor_district']) }} as district_key,    
    supervisor_district as district_name 
FROM (
    SELECT DISTINCT
        supervisor_district
    FROM {{ ref('base_fire_incidents') }} 
    WHERE supervisor_district IS NOT NULL 
) AS unique_districts
ORDER BY supervisor_district 
