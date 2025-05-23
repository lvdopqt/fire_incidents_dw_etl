SELECT
    incidentnumber::TEXT AS incident_number,
    exposurenumber::INTEGER AS exposure_number,
    id::TEXT AS original_id,
    address::TEXT AS address,
    incidentdate::DATE AS incident_date, -- Removed TRIM and NULLIF
    callnumber::INTEGER AS call_number,
    alarmdttm::TIMESTAMP AS alarm_dttm, -- Removed TRIM and NULLIF
    arrivaldttm::TIMESTAMP AS arrival_dttm, -- Removed TRIM and NULLIF
    closedttm::TIMESTAMP AS close_dttm, -- Removed TRIM and NULLIF
    city::TEXT AS city_name,
    zipcode::TEXT AS zipcode,
    battalion::TEXT AS battalion_name,
    stationarea::TEXT AS station_area,
    box::TEXT AS box_area,
    suppressionunits::INTEGER AS suppression_units,
    suppressionpersonnel::INTEGER AS suppression_personnel,
    emsunits::INTEGER AS ems_units,
    emspersonnel::INTEGER AS ems_personnel,
    otherunits::INTEGER AS other_units,
    otherpersonnel::INTEGER AS other_personnel,
    firstunitonscene::TEXT AS first_unit_arrived,
    -- For NUMERIC fields, keep NULLIF(TRIM(...), '') if they were loaded as TEXT and need handling
    -- but your Python script now handles them as NUMERIC/FLOAT64 to Int64, so they should be clean.
    -- If your Python script loads these directly as NUMERIC, you can simplify them too.
    -- Based on the previous successful load, estimatedpropertyloss is NUMERIC, so we can simplify.
    estimatedpropertyloss::NUMERIC AS est_property_loss, -- Simplified
    estimatedcontentsloss::NUMERIC AS est_contents_loss, -- Simplified
    firefatalities::INTEGER AS fire_fatalities,
    fireinjuries::INTEGER AS fire_injuries,
    civilianfatalities::INTEGER AS civilian_fatalities,
    civilianinjuries::INTEGER AS civilian_injuries,
    numberofalarms::INTEGER AS number_of_alarms,
    primarysituation::TEXT AS primary_situation,
    mutualaid::TEXT AS mutual_aid_response,
    actiontakenprimary::TEXT AS action_taken_primary,
    actiontakensecondary::TEXT AS action_taken_secondary,
    actiontakenother::TEXT AS action_taken_other,
    detectoralertedoccupants::TEXT AS detector_alerted_occupants,
    propertyuse::TEXT AS property_use,
    areaoffireorigin::TEXT AS area_of_fire_origin,
    ignitioncause::TEXT AS ignition_cause,
    ignitionfactorprimary::TEXT AS ignition_factor_primary,
    ignitionfactorsecondary::TEXT AS ignition_factor_secondary,
    heatsource::TEXT AS heat_source,
    itemfirstignited::TEXT AS item_first_ignited,
    humanfactorsassociatedwithignition::TEXT AS human_factors_associated_with_ignition,
    structuretype::TEXT AS structure_type,
    structurestatus::TEXT AS structure_status,
    flooroffireorigin::TEXT AS floor_of_fire_origin,
    firespread::TEXT AS fire_spread,
    noflamespread::NUMERIC AS no_flame_spread, -- Simplified
    numberoffloorswithminimumdamage::INTEGER AS number_of_floors_with_minimum_damage,
    numberoffloorswithsignificantdamage::INTEGER AS number_of_floors_with_significant_damage,
    numberoffloorswithheavydamage::INTEGER AS number_of_floors_with_heavy_damage,
    numberoffloorswithextremedamage::INTEGER AS number_of_floors_with_extreme_damage,
    detectorspresent::TEXT AS detectors_present,
    detectortype::TEXT AS detector_type,
    detectoroperation::TEXT AS detector_operation,
    detectoreffectiveness::TEXT AS detector_effectiveness,
    detectorfailurereason::TEXT AS detector_failure_reason,
    automaticextinguishingsystempresent::TEXT AS automatic_extinguishing_system_present,
    automaticextinguishingsytemtype::TEXT AS automatic_extinguishing_sytem_type,
    automaticextinguishingsytemperfomance::TEXT AS automatic_extinguishing_sytem_performance,
    automaticextinguishingsytemfailurereason::TEXT AS automatic_extinguishing_sytem_failure_reason,
    numberofsprinklerheadsoperating::INTEGER AS number_of_sprinkler_heads_operating,
    supervisordistrict::TEXT AS supervisor_district,
    neighborhood_district::TEXT AS neighborhood_district,
    point::TEXT AS geo_point,
    data_as_of::TIMESTAMP AS data_as_of_dttm, -- Removed TRIM and NULLIF
    data_loaded_at::TIMESTAMP AS data_loaded_at_dttm -- Removed TRIM and NULLIF

FROM {{ source('fire_department_raw', 'stg_fire_incidents_raw') }}
