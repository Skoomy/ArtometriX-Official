"""
Data merger for combining multiple data sources.

Single Responsibility: Orchestrate joins across all data sources
to create a unified forecasting dataset.
"""
from typing import Dict, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataMerger:
    """
    Merges multiple data sources into a unified DataFrame.

    Handles left joins, validation, and missing data reporting.
    """

    def __init__(self):
        """Initialize data merger."""
        self.merge_report: Dict = {}

    def merge_all(
        self,
        historical_volume: pd.DataFrame,
        price_sales_promotion: pd.DataFrame,
        event_calendar: pd.DataFrame,
        weather: pd.DataFrame,
        demographics: pd.DataFrame,
        industry_volume: pd.DataFrame,
        industry_soda_sales: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Merge all data sources into unified dataset.

        Strategy:
        - Start with historical_volume (target variable)
        - Left join price/sales/promotion on (agency, sku, year_month)
        - Left join event_calendar on (year_month)
        - Left join weather on (agency, year_month)
        - Left join demographics on (agency)
        - Left join industry metrics on (year_month)

        Args:
            historical_volume: Historical demand data (base table)
            price_sales_promotion: Pricing and promotion data
            event_calendar: Event calendar with holidays/events
            weather: Weather data by agency and month
            demographics: Demographics by agency
            industry_volume: Industry volume benchmarks
            industry_soda_sales: Industry soda sales benchmarks

        Returns:
            Unified DataFrame with all features
        """
        logger.info("Starting data merge process...")
        self.merge_report = {"initial_rows": len(historical_volume)}

        # Start with historical volume (base table)
        df = historical_volume.copy()
        logger.info(f"Base table (historical_volume): {len(df)} rows")

        # Merge price/sales/promotion (agency, sku, year_month)
        df = self._merge_price_sales_promotion(df, price_sales_promotion)

        # Merge event calendar (year_month)
        df = self._merge_event_calendar(df, event_calendar)

        # Merge weather (agency, year_month)
        df = self._merge_weather(df, weather)

        # Merge demographics (agency)
        df = self._merge_demographics(df, demographics)

        # Merge industry volume (year_month)
        df = self._merge_industry_volume(df, industry_volume)

        # Merge industry soda sales (year_month)
        df = self._merge_industry_soda_sales(df, industry_soda_sales)

        # Final cleanup and sorting
        df = self._finalize_dataframe(df)

        logger.info(f"Merge complete: {len(df)} rows, {len(df.columns)} columns")
        self.merge_report["final_rows"] = len(df)
        self.merge_report["final_columns"] = len(df.columns)

        return df

    def _merge_price_sales_promotion(
        self, df: pd.DataFrame, price_sales_promotion: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge price/sales/promotion data."""
        merge_cols = ["agency", "sku", "year_month"]

        # Select relevant columns (exclude duplicates like year, month)
        psp_cols = [
            "agency",
            "sku",
            "year_month",
            "price",
            "sales",
            "promotions",
            "total_revenue",
            "promo_ratio",
        ]
        psp_df = price_sales_promotion[psp_cols]

        df = df.merge(psp_df, on=merge_cols, how="left", validate="1:1")

        missing = df["price"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing price/sales data for {missing} records")
        self.merge_report["price_sales_missing"] = missing

        return df

    def _merge_event_calendar(self, df: pd.DataFrame, event_calendar: pd.DataFrame) -> pd.DataFrame:
        """Merge event calendar data."""
        merge_cols = ["year_month"]

        # Get all event columns
        event_cols = [
            col
            for col in event_calendar.columns
            if col not in ["year_month", "year", "month"]
        ]

        ec_df = event_calendar[["year_month"] + event_cols]
        df = df.merge(ec_df, on=merge_cols, how="left", validate="m:1")

        missing = df["total_events"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing event calendar data for {missing} records")
        self.merge_report["event_calendar_missing"] = missing

        return df

    def _merge_weather(self, df: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
        """Merge weather data."""
        merge_cols = ["agency", "year_month"]

        weather_cols = ["agency", "year_month", "avg_max_temp", "temp_category"]
        weather_df = weather[weather_cols]

        df = df.merge(weather_df, on=merge_cols, how="left", validate="m:1")

        missing = df["avg_max_temp"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing weather data for {missing} records")
        self.merge_report["weather_missing"] = missing

        return df

    def _merge_demographics(self, df: pd.DataFrame, demographics: pd.DataFrame) -> pd.DataFrame:
        """Merge demographics data."""
        merge_cols = ["agency"]

        demo_cols = [
            "agency",
            "avg_population_2017",
            "avg_yearly_household_income_2017",
            "income_quintile",
            "population_segment",
        ]
        demo_df = demographics[demo_cols]

        df = df.merge(demo_df, on=merge_cols, how="left", validate="m:1")

        missing = df["avg_population_2017"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing demographics data for {missing} records")
        self.merge_report["demographics_missing"] = missing

        return df

    def _merge_industry_volume(
        self, df: pd.DataFrame, industry_volume: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge industry volume benchmarks."""
        merge_cols = ["year_month"]

        iv_cols = [
            "year_month",
            "industry_volume",
            "industry_volume_yoy_growth",
            "industry_volume_mom_growth",
        ]
        iv_df = industry_volume[iv_cols]

        df = df.merge(iv_df, on=merge_cols, how="left", validate="m:1")

        missing = df["industry_volume"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing industry volume data for {missing} records")
        self.merge_report["industry_volume_missing"] = missing

        return df

    def _merge_industry_soda_sales(
        self, df: pd.DataFrame, industry_soda_sales: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge industry soda sales benchmarks."""
        merge_cols = ["year_month"]

        iss_cols = [
            "year_month",
            "soda_volume",
            "soda_volume_yoy_growth",
            "soda_volume_mom_growth",
        ]
        iss_df = industry_soda_sales[iss_cols]

        df = df.merge(iss_df, on=merge_cols, how="left", validate="m:1")

        missing = df["soda_volume"].isna().sum()
        if missing > 0:
            logger.warning(f"Missing industry soda sales data for {missing} records")
        self.merge_report["industry_soda_missing"] = missing

        return df

    def _finalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and sorting."""
        # Sort by agency, sku, and time
        df = df.sort_values(["agency", "sku", "year_month"]).reset_index(drop=True)

        # Ensure consistent column order (logical grouping)
        column_order = self._get_column_order(df.columns.tolist())
        df = df[column_order]

        return df

    def _get_column_order(self, columns: List[str]) -> List[str]:
        """
        Define logical column ordering.

        Groups: identifiers, time, target, price/promo, external factors
        """
        priority_groups = [
            # Identifiers
            ["agency", "sku"],
            # Time
            ["year_month", "year", "month"],
            # Target
            ["volume"],
            # Price and promotions
            ["price", "sales", "promotions", "total_revenue", "promo_ratio"],
            # Events
            [
                "total_events",
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
            ],
            # Weather
            ["avg_max_temp", "temp_category"],
            # Demographics
            [
                "avg_population_2017",
                "avg_yearly_household_income_2017",
                "income_quintile",
                "population_segment",
            ],
            # Industry benchmarks
            [
                "industry_volume",
                "industry_volume_yoy_growth",
                "industry_volume_mom_growth",
                "soda_volume",
                "soda_volume_yoy_growth",
                "soda_volume_mom_growth",
            ],
        ]

        ordered = []
        remaining = set(columns)

        # Add columns in priority order
        for group in priority_groups:
            for col in group:
                if col in remaining:
                    ordered.append(col)
                    remaining.remove(col)

        # Add any remaining columns
        ordered.extend(sorted(remaining))

        return ordered

    def get_merge_report(self) -> Dict:
        """
        Get merge report with statistics.

        Returns:
            Dictionary with merge statistics and warnings
        """
        return self.merge_report.copy()

    def print_merge_report(self) -> None:
        """Print formatted merge report."""
        if not self.merge_report:
            print("No merge report available. Run merge_all() first.")
            return

        print("\n" + "=" * 60)
        print("DATA MERGE REPORT")
        print("=" * 60)
        print(f"Initial rows: {self.merge_report.get('initial_rows', 'N/A')}")
        print(f"Final rows: {self.merge_report.get('final_rows', 'N/A')}")
        print(f"Final columns: {self.merge_report.get('final_columns', 'N/A')}")
        print("\nMissing Data Summary:")
        print(f"  - Price/Sales: {self.merge_report.get('price_sales_missing', 0)} records")
        print(f"  - Events: {self.merge_report.get('event_calendar_missing', 0)} records")
        print(f"  - Weather: {self.merge_report.get('weather_missing', 0)} records")
        print(f"  - Demographics: {self.merge_report.get('demographics_missing', 0)} records")
        print(f"  - Industry Volume: {self.merge_report.get('industry_volume_missing', 0)} records")
        print(f"  - Industry Soda: {self.merge_report.get('industry_soda_missing', 0)} records")
        print("=" * 60 + "\n")
