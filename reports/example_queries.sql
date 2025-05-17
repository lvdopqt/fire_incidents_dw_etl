-- Example Analytical Queries for Fire Incidents Data Warehouse

-- These queries demonstrate how to use the dimension (dim_*) and fact (fct_*)
-- tables to analyze fire incident data.

-- Note: These queries assume your models are in the 'public' schema.

-- Query 1: Total number of incidents per year
-- Aggregates the count of fire incidents by year using the dim_time dimension.
SELECT
    t.year,
    COUNT(f.incident_number) AS total_incidents
FROM public.fct_fire_incidents AS f
INNER JOIN public.dim_time AS t ON f.incident_time_key = t.time_key
GROUP BY t.year
ORDER BY t.year;


-- Query 2: Number of incidents by primary situation and year
-- Breaks down incident counts by their primary situation and the year they occurred.
SELECT
    t.year,
    f.primary_situation,
    COUNT(f.incident_number) AS total_incidents
FROM public.fct_fire_incidents AS f
INNER JOIN public.dim_time AS t ON f.incident_time_key = t.time_key
-- Exclude incidents with no primary situation
WHERE f.primary_situation IS NOT NULL
GROUP BY t.year, f.primary_situation
ORDER BY t.year ASC, total_incidents DESC;


-- Query 3: Average number of suppression units per battalion
-- Aggregates the average number of suppression units dispatched per incident
-- for each battalion using the dim_battalion dimension.
SELECT
    b.battalion_name,
    AVG(f.suppression_units) AS average_suppression_units
FROM public.fct_fire_incidents AS f
INNER JOIN public.dim_battalion AS b ON f.battalion_key = b.battalion_key
-- Exclude incidents with no suppression units reported
WHERE f.suppression_units IS NOT NULL
GROUP BY b.battalion_name
ORDER BY average_suppression_units DESC;
