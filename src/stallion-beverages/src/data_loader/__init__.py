"""
Data Loader Module for Stallion Beverages Forecasting.

This module provides a robust, modular pipeline for loading, validating,
and merging multiple CSV data sources into a unified DataFrame.

Public API:
    DataLoader - Main orchestrator class
    ConfigLoader - Configuration loading and validation
    DataMerger - Data merging logic

    Individual loaders:
    - HistoricalVolumeLoader
    - PriceSalesPromotionLoader
    - EventCalendarLoader
    - WeatherLoader
    - DemographicsLoader
    - IndustryVolumeLoader
    - IndustrySodaSalesLoader

Usage:
    from data_loader import DataLoader

    # Basic usage
    loader = DataLoader()
    df = loader.load_all()

    # Custom configuration
    loader = DataLoader(config_path="custom_config.yaml")
    df = loader.load_all(validate=True)

    # Access individual datasets
    loader.load_all()
    volume_df = loader.get_dataset("historical_volume")
"""

# Import main components
from .loader import DataLoader
from .config import ConfigLoader
from .merger import DataMerger
from .loaders import (
    BaseDataLoader,
    HistoricalVolumeLoader,
    PriceSalesPromotionLoader,
    EventCalendarLoader,
    WeatherLoader,
    DemographicsLoader,
    IndustryVolumeLoader,
    IndustrySodaSalesLoader,
)

# Define public API
__all__ = [
    # Main classes
    "DataLoader",
    "ConfigLoader",
    "DataMerger",
    # Base class
    "BaseDataLoader",
    # Individual loaders
    "HistoricalVolumeLoader",
    "PriceSalesPromotionLoader",
    "EventCalendarLoader",
    "WeatherLoader",
    "DemographicsLoader",
    "IndustryVolumeLoader",
    "IndustrySodaSalesLoader",
]

# Package metadata
__version__ = "0.1.0"
__author__ = "ArtometriX"
