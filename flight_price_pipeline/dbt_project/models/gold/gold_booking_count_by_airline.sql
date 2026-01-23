{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Booking Count by Airline
    
    KPI: Booking volume and trends by airline
*/

SELECT
    airline,
    
    -- Total bookings
    COUNT(*) AS total_bookings,
    
    -- By booking source
    COUNT(*) FILTER (WHERE booking_source = 'Direct Booking') AS direct_bookings,
    COUNT(*) FILTER (WHERE booking_source = 'Travel Agency') AS agency_bookings,
    COUNT(*) FILTER (WHERE booking_source = 'Online Website') AS online_bookings,
    
    -- By booking window
    COUNT(*) FILTER (WHERE booking_window = 'Last Minute') AS last_minute_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Short Notice') AS short_notice_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Advance') AS advance_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Early Bird') AS early_bird_bookings,
    
    -- By season
    COUNT(*) FILTER (WHERE seasonality = 'Regular') AS regular_season_bookings,
    COUNT(*) FILTER (WHERE seasonality = 'Eid') AS eid_bookings,
    COUNT(*) FILTER (WHERE seasonality = 'Hajj') AS hajj_bookings,
    COUNT(*) FILTER (WHERE seasonality = 'Winter') AS winter_bookings,
    
    -- By day type
    COUNT(*) FILTER (WHERE departure_day_type = 'Weekday') AS weekday_bookings,
    COUNT(*) FILTER (WHERE departure_day_type = 'Weekend') AS weekend_bookings,
    
    -- Average days before departure
    ROUND(AVG(days_before_departure), 1) AS avg_days_before_departure,
    
    -- Market share
    ROUND((COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER ()) * 100, 2) AS market_share_pct,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM {{ ref('silver_cleaned_flights') }}
GROUP BY airline
ORDER BY total_bookings DESC
