{{
    config(
        materialized='incremental',
        unique_key=['incident_number', 'exposure_number']
    )
}}

SELECT

    inc.incident_number,
    inc.exposure_number,

    t.time_key AS incident_time_key,
    d.district_key,
    b.battalion_key,

    inc.number_of_alarms,
    inc.primary_situation,
    inc.property_use,
    inc.suppression_units,
    inc.ems_units,
    inc.station_area,
    inc.zipcode,
    inc.city_name,
    inc.geo_point,

    inc.incident_date,
    inc.alarm_dttm,
    inc.arrival_dttm,
    inc.close_dttm,
    inc.data_as_of_dttm,
    inc.data_loaded_at_dttm

FROM {{ ref('stg_fire_incidents') }} AS inc

INNER JOIN {{ ref('dim_time') }} AS t
    ON inc.incident_date = t.date_day

INNER JOIN {{ ref('dim_district') }} AS d
    ON inc.district_name = d.district_name

INNER JOIN {{ ref('dim_battalion') }} AS b
    ON inc.battalion_name = b.battalion_name


{#
    {% if is_incremental() %}
        
    {% endif %}
#}
