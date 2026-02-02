# Flight Price Analysis Pipeline - Architecture Diagrams

## Overview

This directory contains the architecture diagram for the Flight Price Analysis data pipeline system. The diagram shows the complete data flow from raw CSV data through Apache Airflow orchestration, MySQL staging, and PostgreSQL analytics with a medallion architecture pattern.

## Files

### 1. `flight_price_architecture.dot`
- **Format:** GraphViz DOT (text-based diagram source)
- **Size:** ~6 KB
- **Usage:** 
  - Primary source file for the architecture diagram
  - Can be edited with any text editor
  - Compatible with all GraphViz tools and online renderers
  - Version control friendly

### 2. `flight_price_architecture` (temp file)
- Temporary file from diagram generation (can be deleted)

### 3. `viewer.html`
- **Format:** HTML web page
- **Usage:** Open this file in a web browser to view the architecture diagram online
- **Features:**
  - Interactive viewer using Graphviz Online
  - Buttons to open online viewer or download DOT file
  - No local GraphViz installation required for viewing

## How to View the Diagram

### Option 1: Web Browser (Recommended - No Installation Required)
```bash
# Simply open in your browser
viewer.html
```
Then click "Open in Graphviz Online" button.

### Option 2: Online Graphviz Renderer
Upload the `flight_price_architecture.dot` file to:
https://dreampuf.github.io/GraphvizOnline/

### Option 3: Generate PNG Locally (Requires GraphViz)
```bash
# Generate PNG image (300 DPI recommended)
dot -Tpng flight_price_architecture.dot -o flight_price_architecture.png

# Generate SVG (scalable vector)
dot -Tsvg flight_price_architecture.dot -o flight_price_architecture.svg

# Generate PDF
dot -Tpdf flight_price_architecture.dot -o flight_price_architecture.pdf
```

## Architecture Components

### Data Source Layer
- **CSV Source:** Flight_Price_Dataset.csv
  - 57,000 records
  - 18 columns (airline, source, destination, fares, etc.)
  - Bangladesh flight price data

### Airflow Orchestration Layer (Port 8080)
- **Webserver:** Web UI for monitoring and management
- **Scheduler:** Executes DAG tasks according to schedule
- **DAG:** `flight_price_pipeline`

**Tasks (Sequential Execution):**
1. **Task 1 - Load CSV** (~8 seconds)
   - Read CSV with Pandas
   - Rename columns to snake_case
   - Insert 57,000 rows to MySQL raw_flight_data

2. **Task 2 - Validate Data** (~10 seconds)
   - Validate required fields
   - Check positive values
   - Verify IATA codes
   - 100% pass rate
   - Mark records with is_valid flag

3. **Task 3 - Transfer to PostgreSQL** (~12 seconds)
   - Extract valid records only
   - Convert boolean types
   - Load to PostgreSQL Bronze layer

4. **Task 4 - DBT Run + Snapshot** (~4 seconds)
   - Execute Silver transformations
   - Execute Gold aggregations
   - Update SCD Type 2 snapshots
   - Run 22 tests (all passing)

### MySQL Staging (Port 3307)
**Database:** flight_staging

**Tables:**
- `raw_flight_data` - 57,000 raw records, no validation
- `validated_flight_data` - 57,000 records with is_valid flag, foreign key to raw data

### PostgreSQL Analytics (Port 5433)
**Database:** flight_analytics

#### Bronze Layer
- **Table:** validated_flights
- **Records:** 57,000
- **Purpose:** Raw validated data, no transformations applied
- **Schema:** Exact copy of validated MySQL data

#### Silver Layer
- **Table:** silver_cleaned_flights (57,000 rows)
  - Standardized text (uppercase)
  - Derived columns:
    - route (source || destination)
    - fare_category (Budget/Standard/Premium)
    - booking_window (Last Minute/Standard/Advance/Early Bird)
    - route_type (Domestic/International)
    - is_peak_season (boolean)

- **Snapshot: flight_fare_snapshot** (19,052 rows)
  - SCD Type 2 historical tracking
  - Tracks: airline, route, class, seasonality combinations
  - Columns: avg_base_fare, avg_total_fare, booking_count
  - Valid from/to dates for historical analysis

- **Snapshot: route_fare_snapshot** (152 rows)
  - SCD Type 2 historical tracking
  - Tracks: route-level metrics
  - Columns: avg_fare, total_bookings, airline_count

#### Gold Layer (KPI Tables)
1. **gold_avg_fare_by_airline** (24 rows)
   - Fare statistics per airline
   - avg, min, max, median, market_share_pct

2. **gold_seasonal_fare_analysis** (4 rows)
   - Fare comparison across seasons
   - percentage_difference_from_regular

3. **gold_booking_count_by_airline** (24 rows)
   - Booking volume by airline
   - Split by booking_source and booking_window

4. **gold_popular_routes** (152 rows)
   - Top routes by booking count
   - With fare statistics

5. **gold_fare_by_class** (3 rows)
   - Economy, Business, First Class
   - Domestic and international split

6. **gold_data_quality_report** (13 rows)
   - Categorical value distributions
   - Anomaly tracking

#### Audit Schema
- **Table:** pipeline_runs
- Tracks execution history, success/failure, row counts, timing

### DBT Transformation Engine
- **Models:** 9 total
  - 1 Silver model
  - 6 Gold models
  - 2 history views
- **Tests:** 22 (all passing)
- **Snapshots:** 2 (SCD Type 2)
- **Execution Time:** ~4 seconds

## Data Flow Summary

```
CSV (57K rows)
    ↓
Task 1: Load CSV → MySQL raw_flight_data (57K rows)
    ↓
Task 2: Validate → MySQL validated_flight_data (57K rows, is_valid=1)
    ↓
Task 3: Transfer → PostgreSQL Bronze.validated_flights (57K rows)
    ↓
Task 4: DBT Transform → PostgreSQL Silver (cleaned + standardized)
                      ↓
                    SCD Snapshots (19K + 152 rows)
    ↓
Task 4: DBT Aggregate → PostgreSQL Gold (6 KPI tables)
                      ↓
                    Gold Views (2 history views)
```

## Color Coding

- **Light Blue (#E3F2FD):** Data source and MySQL
- **Light Orange (#FFE0B2):** Airflow orchestration
- **Light Green (#E8F5E9):** PostgreSQL analytics
- **Bronze (#D7CCC8):** Bronze medallion layer
- **Silver (#CFD8DC):** Silver medallion layer
- **Gold (#FFF8E1):** Gold medallion layer
- **Light Gray (#ECEFF1):** Docker environment boundary

## Performance Metrics

- **Total Pipeline Duration:** ~34 seconds
- **Data Volume:** 57,000 records
- **Validation Pass Rate:** 100%
- **Airlines:** 24
- **Unique Routes:** 152
- **DBT Models:** 9
- **DBT Tests:** 22 (all passing)
- **SCD Snapshots:** 2

## Regenerating the Diagram

To regenerate the architecture diagram:

```bash
cd docs
python generate_architecture.py
```

This will create:
- `diagrams/flight_price_architecture.dot` - GraphViz source
- `diagrams/flight_price_architecture` - Temporary file (can delete)
- Will attempt to generate PNG (requires GraphViz installed)

## Requirements for PNG Generation

If you want to generate PNG/SVG/PDF locally, install GraphViz:

**Windows:**
```
winget install graphviz
# Or download from https://graphviz.org/download/
```

**macOS:**
```
brew install graphviz
```

**Linux (Ubuntu/Debian):**
```
sudo apt-get install graphviz
```

## Troubleshooting

### Diagram not rendering in browser?
- Clear browser cache
- Try a different browser
- Check that `flight_price_architecture.dot` exists in this directory

### PNG generation fails?
- Ensure GraphViz is installed and `dot` is in your PATH
- Try the online viewer instead (no local installation needed)
- Upload the DOT file to https://dreampuf.github.io/GraphvizOnline/

### Out-of-date diagram?
- Regenerate using: `python generate_architecture.py` from docs folder
- The diagram is automatically generated from the architecture specifications

## References

- **Instructions:** [docs/instructions.md](../instructions.md)
- **Agent Guide:** [docs/agent.md](../agent.md)
- **Main README:** [README.md](../../README.md)
- **GraphViz Docs:** https://graphviz.org/doc/
- **Online Renderer:** https://dreampuf.github.io/GraphvizOnline/

## Notes

- This diagram represents the complete architecture as specified in the instructions
- Colors follow the medallion architecture pattern for clarity
- Arrow colors indicate data flow type (blue=loading, green=transfer, orange=transform, gold=aggregate)
- All component details (row counts, ports, durations) are current as of generation
