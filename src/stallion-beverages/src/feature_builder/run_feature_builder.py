"""
Feature Builder Pipeline Runner.

Loads data using DataLoader and prepares features for modeling.
"""
import logging
from pathlib import Path
from typing import Optional
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_loader import DataLoader
from timing import timing

logger = logging.getLogger(__name__)


@timing
def run_feature_builder(
    config_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
    output_format: str = "parquet",
) -> None:
    """
    Run the feature builder pipeline.

    Args:
        config_path: Path to config.yaml
        output_path: Path to save processed data
        output_format: Output format ('parquet', 'csv', 'feather')
    """
    logger.info("Starting Feature Builder Pipeline...")

    # Initialize data loader
    logger.info("Initializing DataLoader...")
    loader = DataLoader(config_path=config_path)

    # Load and merge all data
    logger.info("Loading all data sources...")
    unified_df = loader.load_all(validate=True)

    # Print summary
    loader.print_summary()

    # Save if output path provided
    if output_path:
        logger.info(f"Saving unified data to {output_path}...")
        loader.save_unified_data(output_path, format=output_format)
    else:
        logger.info("No output path specified, skipping save.")

    logger.info("Feature Builder Pipeline completed successfully!")

    return unified_df


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Feature Builder Pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config.yaml (default: config/config.yaml)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to save unified data (optional)",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["parquet", "csv", "feather"],
        default="parquet",
        help="Output format (default: parquet)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run pipeline
    run_feature_builder(
        config_path=args.config,
        output_path=args.output,
        output_format=args.format,
    )


if __name__ == "__main__":
    main()