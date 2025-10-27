"""
Data Loader Module for Stallion Beverages Forecasting.

Main orchestrator that coordinates config loading, individual data loaders,
and data merging to produce a unified DataFrame for forecasting.

Usage:
    from data_loader import DataLoader

    # Basic usage
    loader = DataLoader()
    df = loader.load_all()

    # Custom config
    loader = DataLoader(config_path="custom_config.yaml")
    df = loader.load_all(validate=True)

    # Access individual datasets
    loader.load_all()
    volume_df = loader.get_dataset("historical_volume")
"""
from pathlib import Path
from typing import Dict, Optional
import logging

import pandas as pd

from .config import ConfigLoader
from .loaders import (
    HistoricalVolumeLoader,
    PriceSalesPromotionLoader,
    EventCalendarLoader,
    WeatherLoader,
    DemographicsLoader,
    IndustryVolumeLoader,
    IndustrySodaSalesLoader,
)
from .merger import DataMerger

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Main data loader orchestrator.

    Responsibilities:
    - Load and validate configuration
    - Coordinate individual data loaders
    - Merge data into unified DataFrame
    - Provide access to individual datasets

    This is the primary entry point for data loading operations.
    """

    def __init__(self, config_path: Optional[Path] = None, base_dir: Optional[Path] = None):
        """
        Initialize DataLoader.

        Args:
            config_path: Path to config.yaml. If None, uses default.
            base_dir: Base directory for resolving relative paths. If None, auto-detects.
        """
        self.config_loader = ConfigLoader(config_path)
        self.base_dir = base_dir
        self._datasets: Dict[str, pd.DataFrame] = {}
        self._unified_df: Optional[pd.DataFrame] = None
        self._loaders: Dict = {}
        self._merger = DataMerger()

    def validate_config(self) -> bool:
        """
        Validate configuration and file existence.

        Returns:
            bool: True if valid

        Raises:
            FileNotFoundError: If config or data files missing
            ValidationError: If config schema invalid
        """
        try:
            self.config_loader.validate_files_exist(self.base_dir)
            logger.info("Configuration validated successfully")
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise

    def _initialize_loaders(self) -> None:
        """Initialize all data loaders with resolved paths."""
        paths = self.config_loader.resolve_data_paths(self.base_dir)

        self._loaders = {
            "historical_volume": HistoricalVolumeLoader(paths["historical_volume"]),
            "price_sales_promotion": PriceSalesPromotionLoader(paths["prices_sales_promotions"]),
            "event_calendar": EventCalendarLoader(paths["event_calendar"]),
            "weather": WeatherLoader(paths["weather"]),
            "demographics": DemographicsLoader(paths["demographics"]),
            "industry_volume": IndustryVolumeLoader(paths["industry_volume"]),
            "industry_soda_sales": IndustrySodaSalesLoader(paths["industry_soda_sales"]),
        }

    def load_dataset(self, dataset_name: str, validate: bool = True) -> pd.DataFrame:
        """
        Load a specific dataset.

        Args:
            dataset_name: Name of dataset to load
            validate: Whether to run Pydantic validation

        Returns:
            Loaded DataFrame

        Raises:
            ValueError: If dataset name unknown
        """
        if not self._loaders:
            self._initialize_loaders()

        if dataset_name not in self._loaders:
            available = ", ".join(self._loaders.keys())
            raise ValueError(f"Unknown dataset: {dataset_name}. Available: {available}")

        logger.info(f"Loading dataset: {dataset_name}")
        df = self._loaders[dataset_name].load(validate=validate)
        self._datasets[dataset_name] = df

        logger.info(f"Loaded {dataset_name}: {len(df)} rows, {len(df.columns)} columns")
        return df

    def load_all(self, validate: bool = True) -> pd.DataFrame:
        """
        Load all datasets and merge into unified DataFrame.

        Args:
            validate: Whether to run Pydantic validation on all datasets

        Returns:
            Unified DataFrame with all features

        Raises:
            FileNotFoundError: If data files missing
            ValueError: If validation fails
        """
        logger.info("Starting data loading process...")

        # Validate configuration first
        self.validate_config()

        # Initialize loaders
        self._initialize_loaders()

        # Load all datasets
        datasets_to_load = [
            "historical_volume",
            "price_sales_promotion",
            "event_calendar",
            "weather",
            "demographics",
            "industry_volume",
            "industry_soda_sales",
        ]

        for dataset_name in datasets_to_load:
            self.load_dataset(dataset_name, validate=validate)

        # Merge all datasets
        logger.info("Merging all datasets...")
        self._unified_df = self._merger.merge_all(
            historical_volume=self._datasets["historical_volume"],
            price_sales_promotion=self._datasets["price_sales_promotion"],
            event_calendar=self._datasets["event_calendar"],
            weather=self._datasets["weather"],
            demographics=self._datasets["demographics"],
            industry_volume=self._datasets["industry_volume"],
            industry_soda_sales=self._datasets["industry_soda_sales"],
        )

        logger.info("Data loading complete!")
        return self._unified_df

    def get_dataset(self, dataset_name: str) -> pd.DataFrame:
        """
        Get a specific loaded dataset.

        Args:
            dataset_name: Name of dataset

        Returns:
            DataFrame for requested dataset

        Raises:
            RuntimeError: If data not loaded yet
            KeyError: If dataset name invalid
        """
        if dataset_name not in self._datasets:
            if not self._datasets:
                raise RuntimeError("Data not loaded. Call load_all() first.")
            raise KeyError(f"Dataset not found: {dataset_name}")

        return self._datasets[dataset_name]

    def get_unified_dataframe(self) -> pd.DataFrame:
        """
        Get the unified/merged DataFrame.

        Returns:
            Unified DataFrame

        Raises:
            RuntimeError: If data not loaded yet
        """
        if self._unified_df is None:
            raise RuntimeError("Unified data not loaded. Call load_all() first.")

        return self._unified_df

    def get_merge_report(self) -> Dict:
        """
        Get merge report with statistics.

        Returns:
            Dictionary with merge statistics
        """
        return self._merger.get_merge_report()

    def print_summary(self) -> None:
        """Print comprehensive summary of loaded data."""
        if self._unified_df is None:
            print("No data loaded. Call load_all() first.")
            return

        print("\n" + "=" * 80)
        print("STALLION BEVERAGES - DATA LOADER SUMMARY")
        print("=" * 80)

        # Unified data summary
        print(f"\nUnified DataFrame:")
        print(f"  Rows: {len(self._unified_df):,}")
        print(f"  Columns: {len(self._unified_df.columns)}")
        print(f"  Memory: {self._unified_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Individual datasets
        print(f"\nIndividual Datasets:")
        for name, df in self._datasets.items():
            print(f"  {name:30} {len(df):>8,} rows × {len(df.columns):>3} cols")

        # Data characteristics
        print(f"\nData Characteristics:")
        print(f"  Unique Agencies: {self._unified_df['agency'].nunique()}")
        print(f"  Unique SKUs: {self._unified_df['sku'].nunique()}")
        print(f"  Date Range: {self._unified_df['year_month'].min()} to {self._unified_df['year_month'].max()}")
        print(f"  Total Months: {self._unified_df['year_month'].nunique()}")

        # Missing data summary
        missing = self._unified_df.isnull().sum()
        missing = missing[missing > 0]
        if len(missing) > 0:
            print(f"\nMissing Values:")
            for col, count in missing.items():
                pct = 100 * count / len(self._unified_df)
                print(f"  {col:30} {count:>8,} ({pct:>5.1f}%)")
        else:
            print(f"\nMissing Values: None")

        # Merge report
        print("")
        self._merger.print_merge_report()

    def save_unified_data(self, output_path: Path, format: str = "parquet") -> None:
        """
        Save unified DataFrame to disk.

        Args:
            output_path: Path to save file
            format: File format ('parquet', 'csv', 'feather')

        Raises:
            RuntimeError: If data not loaded yet
            ValueError: If format unsupported
        """
        if self._unified_df is None:
            raise RuntimeError("No data to save. Call load_all() first.")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving unified data to {output_path}")

        if format == "parquet":
            self._unified_df.to_parquet(output_path, index=False, engine="pyarrow")
        elif format == "csv":
            self._unified_df.to_csv(output_path, index=False)
        elif format == "feather":
            self._unified_df.to_feather(output_path)
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'parquet', 'csv', or 'feather'")

        logger.info(f"Data saved successfully: {output_path}")


# Convenience exports
__all__ = [
    "DataLoader",
    "ConfigLoader",
    "DataMerger",
    "HistoricalVolumeLoader",
    "PriceSalesPromotionLoader",
    "EventCalendarLoader",
    "WeatherLoader",
    "DemographicsLoader",
    "IndustryVolumeLoader",
    "IndustrySodaSalesLoader",
]