"""
Configuration loading and validation.

Handles YAML config parsing with Pydantic validation.
"""
from pathlib import Path
from typing import Dict, Optional
import yaml
from pydantic import BaseModel, Field, field_validator, ConfigDict


class DataSourceConfig(BaseModel):
    """Configuration for data source file paths."""

    model_config = ConfigDict(extra="forbid")

    demographics: str = Field(..., description="Path to demographics CSV")
    event_calendar: str = Field(..., description="Path to event calendar CSV")
    historical_volume: str = Field(..., description="Path to historical volume CSV")
    prices_sales_promotions: str = Field(..., description="Path to price/sales/promo CSV")
    industry_soda_sales: str = Field(..., description="Path to industry soda sales CSV")
    weather: str = Field(..., description="Path to weather CSV")
    industry_volume: str = Field(
        ...,
        alias="industry+volume",  # Handle the + in YAML key
        description="Path to industry volume CSV",
    )

    @field_validator(
        "demographics",
        "event_calendar",
        "historical_volume",
        "prices_sales_promotions",
        "industry_soda_sales",
        "weather",
        "industry_volume",
    )
    @classmethod
    def validate_path_not_empty(cls, v: str) -> str:
        """Ensure paths are not empty strings."""
        if not v or not v.strip():
            raise ValueError("File path cannot be empty")
        return v.strip()


class AppConfig(BaseModel):
    """Main application configuration."""

    model_config = ConfigDict(extra="allow")

    data_source: DataSourceConfig = Field(..., description="Data source configuration")


class ConfigLoader:
    """
    Configuration loader with validation.

    Single Responsibility: Load and validate YAML configuration files.
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config loader.

        Args:
            config_path: Path to config.yaml file. If None, uses default.
        """
        self.config_path = config_path or Path("config/config.yaml")

    def load(self) -> AppConfig:
        """
        Load and validate configuration from YAML file.

        Returns:
            AppConfig: Validated configuration object

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML is malformed
            pydantic.ValidationError: If config doesn't match schema
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r") as f:
            raw_config = yaml.safe_load(f)

        return AppConfig(**raw_config)

    def resolve_data_paths(self, base_dir: Optional[Path] = None) -> Dict[str, Path]:
        """
        Resolve all data source paths to absolute paths.

        Args:
            base_dir: Base directory for relative paths. If None, uses config file directory.

        Returns:
            Dict mapping data source names to resolved Path objects
        """
        config = self.load()
        base = base_dir or self.config_path.parent.parent

        return {
            "demographics": base / config.data_source.demographics,
            "event_calendar": base / config.data_source.event_calendar,
            "historical_volume": base / config.data_source.historical_volume,
            "prices_sales_promotions": base / config.data_source.prices_sales_promotions,
            "industry_soda_sales": base / config.data_source.industry_soda_sales,
            "weather": base / config.data_source.weather,
            "industry_volume": base / config.data_source.industry_volume,
        }

    def validate_files_exist(self, base_dir: Optional[Path] = None) -> bool:
        """
        Validate that all configured data files exist.

        Args:
            base_dir: Base directory for relative paths

        Returns:
            bool: True if all files exist

        Raises:
            FileNotFoundError: If any configured file is missing
        """
        paths = self.resolve_data_paths(base_dir)

        missing_files = []
        for name, path in paths.items():
            if not path.exists():
                missing_files.append(f"{name}: {path}")

        if missing_files:
            raise FileNotFoundError(
                f"Missing data files:\n" + "\n".join(f"  - {f}" for f in missing_files)
            )

        return True
