SELECT
    incident_number::TEXT AS incident_number,
    exposure_number::INT AS exposure_number,
    original_id::TEXT AS incident_id,
    incident_date::DATE AS incident_date,
    alarm_dttm::TIMESTAMP AS alarm_dttm,
    arrival_dttm::TIMESTAMP AS arrival_dttm,
    close_dttm::TIMESTAMP AS close_dttm,
    city_name::TEXT AS city_name,
    zipcode::TEXT AS zipcode,
    battalion_name::TEXT AS battalion_name,
    station_area::TEXT AS station_area,
    supervisor_district::TEXT AS district_name,
    neighborhood_district::TEXT AS neighborhood_district_name,
    geo_point::TEXT AS geo_point,
    data_as_of_dttm::TIMESTAMP AS data_as_of_dttm,
    data_loaded_at_dttm::TIMESTAMP AS data_loaded_at_dttm,

    suppression_units::INT AS suppression_units,
    ems_units::INT AS ems_units,
    number_of_alarms::INT AS number_of_alarms,
    primary_situation::TEXT AS primary_situation,
    property_use::TEXT AS property_use

FROM {{ ref('base_fire_incidents') }}
