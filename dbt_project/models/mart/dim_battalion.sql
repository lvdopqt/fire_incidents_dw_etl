
    SELECT
        {{ dbt_utils.generate_surrogate_key(['battalion_name']) }} as battalion_key,
        battalion_name

    FROM (
        SELECT DISTINCT
            
            battalion_name
        FROM {{ ref('base_fire_incidents') }} 
        WHERE battalion_name IS NOT NULL 
    ) AS unique_battalions
    ORDER BY battalion_name
    