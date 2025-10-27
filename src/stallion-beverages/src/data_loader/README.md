# Data Loader Module

Production-ready data loading framework for Stallion Beverages SKU-level demand forecasting.

## Overview

The data loader module provides a robust, modular pipeline for loading, validating, and merging multiple CSV data sources into a unified DataFrame ready for feature engineering and modeling.

## Architecture

### Design Principles

- **Single Responsibility**: Each loader handles one data source
- **Pydantic Validation**: Strong typing and data validation at schema level
- **Configuration-driven**: YAML-based configuration for flexibility
- **Independent modules**: Can be used standalone or as part of larger pipelines
- **Template Method Pattern**: Consistent loading pipeline across all loaders

### Module Structure

```
data_loader/
├── __init__.py          # Main DataLoader orchestrator
├── config.py            # Configuration loading and validation
├── schemas.py           # Pydantic schemas for data validation
├── loaders.py           # Individual data source loaders
├── merger.py            # Data merging logic
└── README.md           # This file
```

## Components

### 1. Pydantic Schemas (`schemas.py`)

Type-safe data models with validation:

- `HistoricalVolumeRecord` - Sales volume data
- `PriceSalesPromotionRecord` - Pricing and promotion data
- `EventCalendarRecord` - Holiday and event calendar
- `WeatherRecord` - Weather conditions by region
- `DemographicsRecord` - Regional demographics
- `IndustryVolumeRecord` - Industry benchmarks
- `IndustrySodaSalesRecord` - Category benchmarks

### 2. Configuration (`config.py`)

- `ConfigLoader` - Loads and validates YAML configuration
- `DataSourceConfig` - Schema for data source paths
- `AppConfig` - Main application configuration

### 3. Individual Loaders (`loaders.py`)

Each loader extends `BaseDataLoader` and implements:
- Schema validation
- Column normalization
- Custom transformations
- Data quality checks

Available loaders:
- `HistoricalVolumeLoader`
- `PriceSalesPromotionLoader`
- `EventCalendarLoader`
- `WeatherLoader`
- `DemographicsLoader`
- `IndustryVolumeLoader`
- `IndustrySodaSalesLoader`

### 4. Data Merger (`merger.py`)

- `DataMerger` - Orchestrates joins across all data sources
- Left join strategy starting from historical_volume
- Merge validation and reporting
- Missing data tracking

### 5. Main Orchestrator (`__init__.py`)

- `DataLoader` - Primary entry point
- Coordinates config loading, individual loaders, and merging
- Provides unified interface for data access

## Usage

### Basic Usage

```python
from data_loader import DataLoader

# Initialize with default config
loader = DataLoader()

# Load and merge all data
df = loader.load_all()

# Print summary
loader.print_summary()
```

### Custom Configuration

```python
from pathlib import Path
from data_loader import DataLoader

# Use custom config file
loader = DataLoader(
    config_path=Path("custom_config.yaml"),
    base_dir=Path("/path/to/data")
)

df = loader.load_all(validate=True)
```

### Load Individual Datasets

```python
from data_loader import DataLoader

loader = DataLoader()

# Load specific dataset
volume_df = loader.load_dataset("historical_volume")
weather_df = loader.load_dataset("weather")

# Access after full load
loader.load_all()
price_df = loader.get_dataset("price_sales_promotion")
```

### Save Unified Data

```python
from pathlib import Path
from data_loader import DataLoader

loader = DataLoader()
df = loader.load_all()

# Save as parquet (recommended)
loader.save_unified_data(
    output_path=Path("output/unified_data.parquet"),
    format="parquet"
)

# Or CSV
loader.save_unified_data(
    output_path=Path("output/unified_data.csv"),
    format="csv"
)
```

### Validation Control

```python
from data_loader import DataLoader

loader = DataLoader()

# Skip validation for faster loading (development)
df = loader.load_all(validate=False)

# Enable validation (production)
df = loader.load_all(validate=True)
```

## Configuration File

`config/config.yaml` format:

```yaml
data_source:
  demographics: "data/demographics.csv"
  event_calendar: "data/event_calendar.csv"
  historical_volume: "data/historical_volume.csv"
  prices_sales_promotions: "data/price_sales_promotion.csv"
  industry_soda_sales: "data/industry_soda_sales.csv"
  weather: "data/weather.csv"
  industry+volume: "data/industry_volume.csv"
```

## Data Transformations

Each loader applies domain-specific transformations:

### Historical Volume
- Date parsing (year, month extraction)
- Sorting by agency, SKU, time

### Price/Sales/Promotion
- Total revenue calculation
- Promotion ratio calculation
- Date features

### Event Calendar
- Total events per month
- Date features

### Weather
- Temperature categorization (very_cold, cold, moderate, warm, hot)
- Date features

### Demographics
- Income quintiles
- Population segments

### Industry Metrics
- Year-over-year growth rates
- Month-over-month growth rates

## Output Schema

The unified DataFrame contains:

**Identifiers**
- `agency` - Agency ID
- `sku` - Product SKU

**Time Features**
- `year_month` - YYYYMM format
- `year` - Year component
- `month` - Month component

**Target Variable**
- `volume` - Sales volume (units)

**Pricing & Promotions**
- `price` - Product price
- `sales` - Regular sales amount
- `promotions` - Promotional sales amount
- `total_revenue` - Total revenue
- `promo_ratio` - Promotion percentage

**Events** (12 binary flags)
- Holiday indicators
- Special event indicators
- `total_events` - Count per month

**Weather**
- `avg_max_temp` - Average maximum temperature
- `temp_category` - Temperature category

**Demographics**
- `avg_population_2017` - Regional population
- `avg_yearly_household_income_2017` - Household income
- `income_quintile` - Income segment
- `population_segment` - Population segment

**Industry Benchmarks**
- `industry_volume` - Total industry volume
- `industry_volume_yoy_growth` - YoY growth
- `industry_volume_mom_growth` - MoM growth
- `soda_volume` - Soda category volume
- `soda_volume_yoy_growth` - YoY growth
- `soda_volume_mom_growth` - MoM growth

## Error Handling

The module provides clear error messages for:

- **Configuration errors**: Missing or invalid config files
- **Missing data files**: File not found errors with paths
- **Validation errors**: Pydantic validation failures with details
- **Merge issues**: Duplicate keys or missing join matches

## Testing

Run tests:

```bash
# Unit and integration tests
pytest tests/test_data_loader.py -v

# Integration test only
python tests/test_data_loader.py

# From CLI
python src/cli.py runner --pipeline feature_builder
```

## Performance

- **Loading time**: ~2-5 seconds for full pipeline (7 files, 21K+ rows)
- **Memory usage**: ~10-20 MB for unified DataFrame
- **Validation overhead**: ~20-30% slower with Pydantic validation enabled

## Extension

### Adding New Data Sources

1. Create Pydantic schema in `schemas.py`
2. Create loader class in `loaders.py` extending `BaseDataLoader`
3. Add merge logic in `merger.py`
4. Update `DataLoader._initialize_loaders()` in `__init__.py`
5. Add config entry in `config.yaml`

### Custom Transformations

Override `_apply_transformations()` in your loader:

```python
class CustomLoader(BaseDataLoader):
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        # Your custom logic here
        df['custom_feature'] = df['column1'] * df['column2']
        return df
```

## Dependencies

- `pandas>=2.0.0` - DataFrame operations
- `pydantic>=2.0.0` - Data validation
- `pyyaml>=6.0` - YAML parsing
- `pyarrow>=10.0.0` - Parquet support

## License

Internal use only - Stallion Beverages forecasting project.
