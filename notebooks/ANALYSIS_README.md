# NYC Taxi Comprehensive Trip Analysis

## Overview

This notebook provides 18 high-level analyses of NYC taxi trip data (2015-2025) across yellow, green, FHV, and FHVhv cab types, covering **303+ months** and **billions of trips**.

## Analyses Included

### Core Trend Analyses
1. **Multi-Year Trip Volume Trend** - 10-year ride volume evolution with COVID impact
2. **Seasonal Patterns** - Month-over-month seasonality across years
3. **Trip Distance Distribution** - Long-tail behavior and outliers
4. **Fare per Mile Trend** - Pricing evolution and inflation effects
5. **Revenue Estimation** - Market share and total fare trends by taxi type
6. **COVID-19 Impact Analysis** - Lockdown effects and recovery patterns

### Detailed Insights (Requires Extended Sampling)
7. Hour-of-day pickup behavior
8. Tip behavior analysis (by hour, year)
9. Pickup zone popularity ranking
10. Borough-to-borough origin-destination flows
11. Trip duration reliability (traffic congestion trends)
12. Airport analytics (JFK/LGA/EWR)
13. Nightlife mobility index (10pm-4am patterns)
14. Outlier/fraud detection
15. Short vs long trip patterns by hour
16. Surge pricing effects (FHV/FHVhv)
17. Weather correlation (if weather data available)
18. Extreme events impact (storms, holidays)

## Setup

### Prerequisites
- Python 3.11+ with virtual environment
- AWS credentials configured in `.env`
- Curated S3 data available in `s3://nyc-yellowcab-data-as-2025/tlc/curated/`

### Installation

```powershell
# Run setup script
.\scripts\setup_analysis.ps1
```

This will:
- Activate virtual environment
- Load AWS credentials from `.env`
- Install required packages (`scipy`, `python-dotenv`, `jupyter`)
- Create output directories

### Manual Setup

```powershell
# Activate virtual environment
.\sparkprojenv\Scripts\Activate.ps1

# Load environment variables
. .\scripts\load_env.ps1

# Install dependencies
pip install python-dotenv scipy jupyter

# Create output directory
New-Item -ItemType Directory -Path data\local_output\analytics -Force
```

## Running the Analysis

### Option 1: Jupyter Notebook
```powershell
jupyter notebook notebooks/comprehensive_trip_analysis.ipynb
```

### Option 2: VS Code
1. Open `notebooks/comprehensive_trip_analysis.ipynb` in VS Code
2. Select Python kernel: `sparkprojenv`
3. Run cells sequentially

### Option 3: Run All Cells Automatically
```powershell
jupyter nbconvert --to notebook --execute notebooks/comprehensive_trip_analysis.ipynb
```

## Outputs

All plots are saved to `data/local_output/analytics/`:

- `01_trip_volume_trend.png` - Multi-year volume trends
- `02_seasonal_patterns.png` - Monthly seasonality
- `03_distance_distribution.png` - Trip distance histograms
- `04_fare_per_mile_trend.png` - Pricing trends
- `05_revenue_estimation.png` - Revenue and market share
- `06_covid_impact.png` - COVID-19 analysis
- `summary_statistics.csv` - Overall statistics table
- `monthly_summary_full.csv` - Complete monthly aggregations

## Data Sources

- **Raw Data**: S3 `tlc/raw/` (yellow, green, fhv, fhvhv)
- **Curated Data**: S3 `tlc/curated/` (ETL-processed parquet files)
- **Date Range**: 2015-01 through 2025-09
- **Total Months**: 303+ across all cab types

## Key Insights Expected

### Industry Shifts
- FHV/FHVhv growth vs yellow/green taxi decline (2015-2025)
- COVID-19 collapse (March-April 2020: 80-90% drop)
- Post-pandemic recovery patterns (asymmetric across taxi types)

### Seasonal & Temporal
- Winter dips (Jan-Feb), summer peaks (Jun-Aug)
- Holiday effects (Thanksgiving, New Year's)
- Hour-of-day: yellow peaks daytime, FHV peaks nighttime

### Pricing & Economics
- Fare per mile inflation trends
- Revenue shift from yellow cabs to FHV services
- Tip behavior changes (card vs cash adoption)

### Geographic Patterns
- Manhattan dominates yellow/green pickups
- Outer boroughs rely on FHV/FHVhv
- Airport demand (JFK, LGA, EWR) as major revenue driver

## Architecture

```
notebooks/comprehensive_trip_analysis.ipynb
├── Setup & Environment (AWS credentials, imports)
├── Data Loading Functions (S3 parquet readers)
├── Aggregation Engine (monthly summaries)
└── Analysis Sections (1-18)
    ├── Plotting (matplotlib, seaborn)
    ├── Statistical Analysis (scipy, pandas)
    └── Export (PNG plots, CSV summaries)
```

## Performance Notes

- **Memory**: Analysis loads aggregated summaries (not full trip data) to avoid memory issues
- **Sampling**: Distribution analyses use random sampling (10K trips per cab type)
- **Runtime**: Full notebook execution: ~10-15 minutes (depends on S3 speed)
- **Optimization**: Pre-aggregated monthly summaries reduce S3 reads

## Extending the Analysis

To add new analyses:

1. Load trip-level data (not just aggregates):
   ```python
   df = read_parquet_from_s3(key)
   ```

2. Parse timestamps for hourly/daily patterns:
   ```python
   df['pickup_hour'] = pd.to_datetime(df['pickup_datetime']).dt.hour
   ```

3. Join with NYC taxi zone shapefiles for geographic analysis

4. Implement anomaly detection (IQR, Z-score, isolation forest)

## Troubleshooting

### AWS Credentials Error
```
Ensure .env file contains:
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-1
```

### Memory Issues
- Reduce sampling size in distribution analyses
- Process cab types sequentially instead of parallel
- Use Dask for large-scale aggregations

### Missing Dependencies
```powershell
pip install -r requirements.txt
```

### Kernel Not Found in VS Code
1. Install Jupyter extension
2. Select kernel: `sparkprojenv` (Python 3.11)
3. Reload window if needed

## References

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Taxi Zone Shapefile: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
- AWS S3 Documentation: https://docs.aws.amazon.com/s3/

## License

See `LICENSE` in repository root.
