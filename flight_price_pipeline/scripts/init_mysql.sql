-- MySQL Initialization Script
-- Flight Price Dataset of Bangladesh

USE flight_staging;

DROP TABLE IF EXISTS validated_flight_data;
DROP TABLE IF EXISTS raw_flight_data;

CREATE TABLE raw_flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source_code VARCHAR(10) NOT NULL,
    source_name VARCHAR(255),
    destination_code VARCHAR(10) NOT NULL,
    destination_name VARCHAR(255),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2),
    tax_surcharge_bdt DECIMAL(12, 2),
    total_fare_bdt DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

CREATE TABLE validated_flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source_code VARCHAR(10) NOT NULL,
    source_name VARCHAR(255),
    destination_code VARCHAR(10) NOT NULL,
    destination_name VARCHAR(255),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2),
    tax_surcharge_bdt DECIMAL(12, 2),
    total_fare_bdt DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    raw_id INT NOT NULL,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_id) REFERENCES raw_flight_data(id)
);

CREATE INDEX idx_raw_airline ON raw_flight_data(airline);
CREATE INDEX idx_raw_source ON raw_flight_data(source_code);
CREATE INDEX idx_raw_destination ON raw_flight_data(destination_code);
CREATE INDEX idx_validated_airline ON validated_flight_data(airline);
CREATE INDEX idx_validated_route ON validated_flight_data(source_code, destination_code);
CREATE INDEX idx_validated_valid ON validated_flight_data(is_valid);
