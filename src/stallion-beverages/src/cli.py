import click
import logging
from pathlib import Path
from timing import timing

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version="stallion-beverages 0.0.1")
def cli():
    """Stallion Beverages CLI - SKU-level demand forecasting."""
    pass


@cli.command()
@click.option(
    "--pipeline",
    help="Name of the pipeline to run",
    type=click.Choice(["feature_builder"], case_sensitive=False),
    required=True,
)
@click.option(
    "--config",
    help="Path to the config file",
    type=click.Path(exists=True),
    default="config/config.yaml",
)
@click.option(
    "--output",
    help="Path to save output file (optional)",
    type=click.Path(),
    default=None,
)
@click.option(
    "--format",
    help="Output format",
    type=click.Choice(["parquet", "csv", "feather","xlsx"], case_sensitive=False),
    default="parquet",
)
@timing
def runner(pipeline: str, config: str, output: str, format: str):
    """Run a specific pipeline."""


    if pipeline =='data_loader':
        from data_loader.run_data_loader import run_data_loader
        logger.info("Running data loader pipeline")
        config_path = Path(config) if config else None
        output_path = Path(output) if output else None

        run_data_loader(
            config_path=config_path,
            output_path=output_path,
            output_format=format,
        )   

    if pipeline == "feature_builder":
        from feature_builder.run_feature_builder import run_feature_builder

        logger.info("Running feature builder pipeline")
        config_path = Path(config) if config else None
        output_path = Path(output) if output else None

        run_feature_builder(
            config_path=config_path,
            output_path=output_path,
            output_format=format,
        )
    else:
        logger.error(f"Pipeline {pipeline} not found")
        exit(1)


if __name__ == "__main__":
    cli()