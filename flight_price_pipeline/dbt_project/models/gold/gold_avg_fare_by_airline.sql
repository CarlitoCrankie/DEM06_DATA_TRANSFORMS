{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH airline_metrics AS (
    SELECT
        airline,
        COUNT(*) AS total_bookings,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS fare_std_dev,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
        ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS fare_25th_percentile,
        ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS fare_75th_percentile,
        COUNT(*) FILTER (WHERE travel_class = 'Economy') AS economy_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'Business') AS business_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'First') AS first_class_bookings,
        COUNT(*) FILTER (WHERE route_type = 'Domestic') AS domestic_bookings,
        COUNT(*) FILTER (WHERE route_type = 'International') AS international_bookings,
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY airline
)

SELECT
    airline,
    total_bookings,
    avg_base_fare,
    avg_tax_surcharge,
    avg_total_fare,
    min_fare,
    max_fare,
    fare_std_dev,
    median_fare,
    fare_25th_percentile,
    fare_75th_percentile,
    economy_bookings,
    business_bookings,
    first_class_bookings,
    domestic_bookings,
    international_bookings,
    avg_duration_hrs,
    ROUND((total_bookings::numeric / SUM(total_bookings) OVER ()) * 100, 2) AS market_share_pct,
    CURRENT_TIMESTAMP AS generated_at
FROM airline_metrics
ORDER BY total_bookings DESC
