{{
    config(
        materialized='table',
        schema='gold'
    )
}}

SELECT
    route,
    source_code,
    MAX(source_name) AS source_name,
    destination_code,
    MAX(destination_name) AS destination_name,
    route_type,
    COUNT(*) AS total_bookings,
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare,
    ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
    ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
    ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs,
    COUNT(*) FILTER (WHERE num_stops = 0) AS direct_flights,
    COUNT(*) FILTER (WHERE num_stops = 1) AS one_stop_flights,
    COUNT(*) FILTER (WHERE num_stops = 2) AS two_stop_flights,
    COUNT(*) FILTER (WHERE travel_class = 'Economy') AS economy_bookings,
    COUNT(*) FILTER (WHERE travel_class = 'Business') AS business_bookings,
    COUNT(*) FILTER (WHERE travel_class = 'First') AS first_class_bookings,
    MODE() WITHIN GROUP (ORDER BY airline) AS most_common_airline,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank,
    CURRENT_TIMESTAMP AS generated_at
FROM {{ ref('silver_cleaned_flights') }}
GROUP BY 
    route, 
    source_code, 
    destination_code,
    route_type
ORDER BY total_bookings DESC
