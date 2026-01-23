{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH seasonal_metrics AS (
    SELECT
        seasonality,
        is_peak_season,
        COUNT(*) AS total_bookings,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS fare_std_dev,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'Domestic')::numeric, 2) AS avg_domestic_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'International')::numeric, 2) AS avg_international_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'Economy')::numeric, 2) AS avg_economy_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'Business')::numeric, 2) AS avg_business_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'First')::numeric, 2) AS avg_first_class_fare
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY seasonality, is_peak_season
),

regular_avg AS (
    SELECT avg_fare FROM seasonal_metrics WHERE seasonality = 'Regular'
),

with_comparison AS (
    SELECT
        sm.*,
        ROUND(
            ((sm.avg_fare - ra.avg_fare) / NULLIF(ra.avg_fare, 0)) * 100,
            2
        ) AS pct_diff_from_regular
    FROM seasonal_metrics sm
    CROSS JOIN regular_avg ra
)

SELECT
    seasonality,
    is_peak_season,
    total_bookings,
    avg_fare,
    min_fare,
    max_fare,
    fare_std_dev,
    median_fare,
    avg_domestic_fare,
    avg_international_fare,
    avg_economy_fare,
    avg_business_fare,
    avg_first_class_fare,
    pct_diff_from_regular,
    CURRENT_TIMESTAMP AS generated_at
FROM with_comparison
ORDER BY 
    CASE seasonality 
        WHEN 'Regular' THEN 1 
        WHEN 'Eid' THEN 2 
        WHEN 'Hajj' THEN 3 
        WHEN 'Winter' THEN 4 
    END
