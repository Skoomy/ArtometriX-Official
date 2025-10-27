"""
Individual data source loaders.

Each loader follows Single Responsibility Principle - one class per data source.
Each loader handles: file reading, schema validation, and basic transformations.
"""
from pathlib import Path
from typing import List, Type, TypeVar
from abc import ABC, abstractmethod
import pandas as pd
from pydantic import BaseModel, ValidationError

from .schemas import (
    HistoricalVolumeRecord,
    PriceSalesPromotionRecord,
    EventCalendarRecord,
    WeatherRecord,
    DemographicsRecord,
    IndustryVolumeRecord,
    IndustrySodaSalesRecord,
)

T = TypeVar("T", bound=BaseModel)


class BaseDataLoader(ABC):
    """
    Abstract base class for data loaders.

    Implements Template Method pattern for consistent loading pipeline.
    """

    def __init__(self, file_path: Path):
        """
        Initialize loader with file path.

        Args:
            file_path: Path to CSV file
        """
        self.file_path = Path(file_path)
        self._dataframe: pd.DataFrame | None = None

    @property
    @abstractmethod
    def schema_model(self) -> Type[BaseModel]:
        """Return the Pydantic model for this data source."""
        pass

    @property
    @abstractmethod
    def column_mapping(self) -> dict:
        """Return mapping from CSV columns to schema field names."""
        pass

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names to snake_case and apply mapping.

        Args:
            df: Raw DataFrame

        Returns:
            DataFrame with normalized column names
        """
        # Convert to snake_case
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(r"[\s\-]+", "_", regex=True)
        )

        # Apply custom mapping if provided
        if self.column_mapping:
            df = df.rename(columns=self.column_mapping)

        return df

    def _validate_records(self, df: pd.DataFrame) -> List[ValidationError]:
        """
        Validate all records using Pydantic schema.

        Args:
            df: DataFrame to validate

        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []
        for idx, row in df.iterrows():
            try:
                self.schema_model(**row.to_dict())
            except ValidationError as e:
                errors.append({"row": idx, "errors": e.errors()})

        return errors

    def load(self, validate: bool = True) -> pd.DataFrame:
        """
        Load CSV file and optionally validate.

        Args:
            validate: Whether to run Pydantic validation

        Returns:
            Loaded and normalized DataFrame

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If validation fails
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"Data file not found: {self.file_path}")

        # Read CSV
        df = pd.read_csv(self.file_path)

        # Normalize column names
        df = self._normalize_column_names(df)

        # Apply custom transformations
        df = self._apply_transformations(df)

        # Validate if requested
        if validate:
            validation_errors = self._validate_records(df)
            if validation_errors:
                error_summary = f"Validation failed for {len(validation_errors)} records"
                raise ValueError(f"{error_summary}\nFirst error: {validation_errors[0]}")

        self._dataframe = df
        return df

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply loader-specific transformations.

        Override in subclasses for custom logic.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        return df

    @property
    def data(self) -> pd.DataFrame:
        """
        Get loaded DataFrame.

        Returns:
            Loaded DataFrame

        Raises:
            RuntimeError: If data hasn't been loaded yet
        """
        if self._dataframe is None:
            raise RuntimeError("Data not loaded. Call load() first.")
        return self._dataframe


class HistoricalVolumeLoader(BaseDataLoader):
    """Loader for historical volume (demand) data."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return HistoricalVolumeRecord

    @property
    def column_mapping(self) -> dict:
        return {"yearmonth": "year_month"}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add date parsing and sorting."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100
        df = df.sort_values(["agency", "sku", "year_month"]).reset_index(drop=True)
        return df


class PriceSalesPromotionLoader(BaseDataLoader):
    """Loader for price, sales, and promotion data."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return PriceSalesPromotionRecord

    @property
    def column_mapping(self) -> dict:
        return {"yearmonth": "year_month"}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add calculated fields and sorting."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100
        df["total_revenue"] = df["sales"] + df["promotions"]
        df["promo_ratio"] = df["promotions"] / (df["total_revenue"] + 1e-9)
        df = df.sort_values(["agency", "sku", "year_month"]).reset_index(drop=True)
        return df


class EventCalendarLoader(BaseDataLoader):
    """Loader for event calendar data."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return EventCalendarRecord

    @property
    def column_mapping(self) -> dict:
        return {
            "yearmonth": "year_month",
            "fifa_u_17_world_cup": "fifa_u17_world_cup",
            "regional_games_": "regional_games",  # Handle trailing space in CSV
        }

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add date fields and event count."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100

        # Count total events per month
        event_cols = [
            "easter_day",
            "good_friday",
            "new_year",
            "christmas",
            "labor_day",
            "independence_day",
            "revolution_day_memorial",
            "regional_games",
            "fifa_u17_world_cup",
            "football_gold_cup",
            "beer_capital",
            "music_fest",
        ]
        df["total_events"] = df[event_cols].sum(axis=1)

        return df.sort_values("year_month").reset_index(drop=True)


class WeatherLoader(BaseDataLoader):
    """Loader for weather data."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return WeatherRecord

    @property
    def column_mapping(self) -> dict:
        return {"yearmonth": "year_month"}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add date fields and temperature categories."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100

        # Temperature categories for feature engineering
        df["temp_category"] = pd.cut(
            df["avg_max_temp"],
            bins=[-float("inf"), 15, 20, 25, 30, float("inf")],
            labels=["very_cold", "cold", "moderate", "warm", "hot"],
        )

        return df.sort_values(["agency", "year_month"]).reset_index(drop=True)


class DemographicsLoader(BaseDataLoader):
    """Loader for demographics data."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return DemographicsRecord

    @property
    def column_mapping(self) -> dict:
        return {}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add demographic segments."""
        # Income quintiles
        df["income_quintile"] = pd.qcut(
            df["avg_yearly_household_income_2017"], q=5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"]
        )

        # Population segments
        df["population_segment"] = pd.cut(
            df["avg_population_2017"],
            bins=[0, 1.7e6, 2.0e6, 2.5e6, float("inf")],
            labels=["small", "medium", "large", "xlarge"],
        )

        return df.sort_values("agency").reset_index(drop=True)


class IndustryVolumeLoader(BaseDataLoader):
    """Loader for industry volume benchmarks."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return IndustryVolumeRecord

    @property
    def column_mapping(self) -> dict:
        return {"yearmonth": "year_month"}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add time features and trends."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100

        # YoY growth
        df["industry_volume_yoy_growth"] = df["industry_volume"].pct_change(12)

        # MoM growth
        df["industry_volume_mom_growth"] = df["industry_volume"].pct_change(1)

        return df.sort_values("year_month").reset_index(drop=True)


class IndustrySodaSalesLoader(BaseDataLoader):
    """Loader for industry soda sales benchmarks."""

    @property
    def schema_model(self) -> Type[BaseModel]:
        return IndustrySodaSalesRecord

    @property
    def column_mapping(self) -> dict:
        return {"yearmonth": "year_month"}

    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add time features and trends."""
        df["year"] = df["year_month"] // 100
        df["month"] = df["year_month"] % 100

        # YoY growth
        df["soda_volume_yoy_growth"] = df["soda_volume"].pct_change(12)

        # MoM growth
        df["soda_volume_mom_growth"] = df["soda_volume"].pct_change(1)

        return df.sort_values("year_month").reset_index(drop=True)
