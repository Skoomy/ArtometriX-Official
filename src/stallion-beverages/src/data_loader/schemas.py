"""
Pydantic schemas for data validation.

Each schema represents a specific data source with type validation
and business logic constraints.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict


class HistoricalVolumeRecord(BaseModel):
    """Schema for historical volume data (demand/sales)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    agency: str = Field(..., pattern=r"^Agency_\d+$", description="Agency identifier")
    sku: str = Field(..., pattern=r"^SKU_\d+$", description="SKU identifier")
    year_month: int = Field(..., ge=201301, le=999912, description="YYYYMM format")
    volume: float = Field(..., ge=0.0, description="Sales volume in units")

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        """Validate year_month is in valid YYYYMM format."""
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month in year_month: {v}")
        if not (2000 <= year <= 2100):
            raise ValueError(f"Invalid year in year_month: {v}")
        return v


class PriceSalesPromotionRecord(BaseModel):
    """Schema for price, sales, and promotion data."""

    model_config = ConfigDict(str_strip_whitespace=True)

    agency: str = Field(..., pattern=r"^Agency_\d+$")
    sku: str = Field(..., pattern=r"^SKU_\d+$")
    year_month: int = Field(..., ge=201301, le=999912)
    price: float = Field(..., ge=0.0, description="Product price")
    sales: float = Field(..., ge=0.0, description="Regular sales amount")
    promotions: float = Field(..., ge=0.0, description="Promotional sales amount")

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {v}")
        if not (2000 <= year <= 2100):
            raise ValueError(f"Invalid year: {v}")
        return v


class EventCalendarRecord(BaseModel):
    """Schema for event calendar data."""

    model_config = ConfigDict(str_strip_whitespace=True)

    year_month: int = Field(..., ge=201301, le=999912)
    easter_day: int = Field(..., ge=0, le=1, description="Binary flag for Easter")
    good_friday: int = Field(..., ge=0, le=1)
    new_year: int = Field(..., ge=0, le=1)
    christmas: int = Field(..., ge=0, le=1)
    labor_day: int = Field(..., ge=0, le=1)
    independence_day: int = Field(..., ge=0, le=1)
    revolution_day_memorial: int = Field(..., ge=0, le=1)
    regional_games: int = Field(..., ge=0, le=1)
    fifa_u17_world_cup: int = Field(..., ge=0, le=1)
    football_gold_cup: int = Field(..., ge=0, le=1)
    beer_capital: int = Field(..., ge=0, le=1)
    music_fest: int = Field(..., ge=0, le=1)

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {v}")
        return v


class WeatherRecord(BaseModel):
    """Schema for weather data."""

    model_config = ConfigDict(str_strip_whitespace=True)

    year_month: int = Field(..., ge=201301, le=999912)
    agency: str = Field(..., pattern=r"^Agency_\d+$")
    avg_max_temp: float = Field(..., ge=-50.0, le=60.0, description="Avg max temperature in Celsius")

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {v}")
        return v


class DemographicsRecord(BaseModel):
    """Schema for demographics data."""

    model_config = ConfigDict(str_strip_whitespace=True)

    agency: str = Field(..., pattern=r"^Agency_\d+$")
    avg_population_2017: int = Field(..., gt=0, description="Average population")
    avg_yearly_household_income_2017: int = Field(..., gt=0, description="Average household income")


class IndustryVolumeRecord(BaseModel):
    """Schema for industry volume benchmarks."""

    model_config = ConfigDict(str_strip_whitespace=True)

    year_month: int = Field(..., ge=201301, le=999912)
    industry_volume: int = Field(..., ge=0, description="Total industry volume")

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {v}")
        return v


class IndustrySodaSalesRecord(BaseModel):
    """Schema for industry soda sales benchmarks."""

    model_config = ConfigDict(str_strip_whitespace=True)

    year_month: int = Field(..., ge=201301, le=999912)
    soda_volume: int = Field(..., ge=0, description="Total soda category volume")

    @field_validator("year_month")
    @classmethod
    def validate_year_month(cls, v: int) -> int:
        year = v // 100
        month = v % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month: {v}")
        return v
