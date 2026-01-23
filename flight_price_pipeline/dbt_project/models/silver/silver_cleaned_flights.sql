{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Silver Layer: Cleaned Flights
    
    This model:
    1. Filters to only valid records
    2. Standardizes text fields (proper case)
    3. Adds derived columns (route, fare calculations)
    4. Adds time-based dimensions for analysis
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'validated_flights') }}
    WHERE is_valid = TRUE
),

cleaned AS (
    SELECT
        -- Primary key
        id,
        
        -- Airline (standardized)
        INITCAP(TRIM(airline)) AS airline,
        
        -- Source airport
        UPPER(TRIM(source_code)) AS source_code,
        INITCAP(TRIM(source_name)) AS source_name,
        
        -- Destination airport
        UPPER(TRIM(destination_code)) AS destination_code,
        INITCAP(TRIM(destination_name)) AS destination_name,
        
        -- Derived: Route
        UPPER(TRIM(source_code)) || '-' || UPPER(TRIM(destination_code)) AS route,
        
        -- Time information
        departure_datetime,
        arrival_datetime,
        duration_hrs,
        
        -- Derived: Time dimensions
        EXTRACT(YEAR FROM departure_datetime) AS departure_year,
        EXTRACT(MONTH FROM departure_datetime) AS departure_month,
        EXTRACT(DOW FROM departure_datetime) AS departure_day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM departure_datetime) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS departure_day_type,
        
        -- Flight details
        stopovers,
        CASE 
            WHEN stopovers = 'Direct' THEN 0
            WHEN stopovers = '1 Stop' THEN 1
            WHEN stopovers = '2 Stops' THEN 2
            ELSE NULL
        END AS num_stops,
        aircraft_type,
        travel_class,
        booking_source,
        
        -- Fare information
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt,
        
        -- Derived: Fare calculations
        ROUND((tax_surcharge_bdt / NULLIF(base_fare_bdt, 0)) * 100, 2) AS tax_percentage,
        CASE
            WHEN total_fare_bdt < 10000 THEN 'Budget'
            WHEN total_fare_bdt < 50000 THEN 'Mid-Range'
            WHEN total_fare_bdt < 100000 THEN 'Premium'
            ELSE 'Luxury'
        END AS fare_category,
        
        -- Booking context
        seasonality,
        CASE
            WHEN seasonality IN ('Eid', 'Hajj', 'Winter') THEN TRUE
            ELSE FALSE
        END AS is_peak_season,
        days_before_departure,
        CASE
            WHEN days_before_departure <= 7 THEN 'Last Minute'
            WHEN days_before_departure <= 14 THEN 'Short Notice'
            WHEN days_before_departure <= 30 THEN 'Advance'
            ELSE 'Early Bird'
        END AS booking_window,
        
        -- Derived: Route type
        CASE
            WHEN source_code IN ('DAC', 'CGP', 'ZYL', 'CXB', 'JSR', 'RJH', 'SPD', 'BZL')
                AND destination_code IN ('DAC', 'CGP', 'ZYL', 'CXB', 'JSR', 'RJH', 'SPD', 'BZL')
            THEN 'Domestic'
            ELSE 'International'
        END AS route_type,
        
        -- Lineage
        mysql_raw_id,
        mysql_validated_id,
        mysql_loaded_at,
        bronze_loaded_at,
        CURRENT_TIMESTAMP AS silver_loaded_at
        
    FROM source_data
)

SELECT * FROM cleaned
