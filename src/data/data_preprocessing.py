import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import duckdb
import os
from utils import configure_s3


def make_parquet(bucket: str, input_path: str, output_path: str = None) -> str:
    """Convert CSV to Parquet on S3 using DuckDB."""
    logger = logging.getLogger(__name__)

    if output_path is None:
        output_path = input_path.replace(".csv", ".parquet")

    con = duckdb.connect(database=":memory:")
    configure_s3(con)

    logger.info(f"Converting s3://{bucket}/{input_path} → s3://{bucket}/{output_path}")
    con.sql(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('s3://{bucket}/{input_path}')
        )
        TO 's3://{bucket}/{output_path}'
        (FORMAT PARQUET)
    """)

    logger.info(f"Parquet saved to s3://{bucket}/{output_path}")
    con.close()
    return output_path


def load_data(path: str) -> duckdb.DuckDBPyRelation:
    """Load dataset from Parquet entirely in DuckDB (local path or s3://)."""
    logger = logging.getLogger(__name__)
    path_str = str(path)
    logger.info(f"Loading data from {path_str}")
    con = duckdb.connect(":memory:")
    if path_str.startswith("s3://"):
        configure_s3(con)
    rel = con.sql(f"SELECT * FROM read_parquet('{path_str}')")

    # Validation SQL
    row_count = rel.aggregate("count(*)").fetchone()[0]
    if row_count == 0:
        logger.error("Parquet file is empty")
        raise ValueError("Parquet is empty")

    cols = rel.columns
    expected_cols = ["country", "year", "region", "cost_healthy_diet_ppp_usd"]
    missing_cols = [col for col in expected_cols if col not in cols]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing columns: {missing_cols}")

    valid_rows = (
        rel.filter("cost_healthy_diet_ppp_usd IS NOT NULL")
        .aggregate("count(*)")
        .fetchone()[0]
    )
    if valid_rows == 0:
        logger.error("No valid cost data found")
        raise ValueError("All cost values are NaN")

    logger.info(f"Loaded {row_count:,} rows, {len(cols)} columns")
    return rel


def basic_overview(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Return compact overview using SQL."""
    return rel.sql(
        """
        SELECT 
            count(*) as rows,
            count(DISTINCT country) as unique_countries,
            count(DISTINCT region) as unique_regions,
            sum(CASE WHEN cost_healthy_diet_ppp_usd IS NULL THEN 1 ELSE 0 END) as null_costs
        FROM {}
    """.format(rel)
    )


NUMERIC_COLS = [
    "cost_healthy_diet_ppp_usd",
    "annual_cost_healthy_diet_usd",
    "cost_vegetables_ppp_usd",
    "cost_fruits_ppp_usd",
    "total_food_components_cost",
]


def clean_data(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Clean and engineer features entirely in SQL."""
    logger = logging.getLogger(__name__)

    return rel.sql(
        """
        SELECT *,
            -- Cast numerics safely
            CAST(cost_healthy_diet_ppp_usd AS DOUBLE) as cost_healthy_diet_ppp_usd,
            CAST(annual_cost_healthy_diet_usd AS DOUBLE) as annual_cost_healthy_diet_usd,
            CAST(year AS INTEGER) as year,

            -- Feature engineering
            (COALESCE(cost_vegetables_ppp_usd, 0) + COALESCE(cost_fruits_ppp_usd, 0)) as food_components_sum,
            (cost_healthy_diet_ppp_usd * 365) as annual_from_ppp_usd,
            (annual_cost_healthy_diet_usd - (cost_healthy_diet_ppp_usd * 365)) as annual_gap_usd,

            -- YoY % change
            (cost_healthy_diet_ppp_usd / 
             LAG(cost_healthy_diet_ppp_usd) OVER (PARTITION BY country ORDER BY year) - 1) * 100 as yoy_pct,

            -- Fill categories
            COALESCE(cost_category, 'Unknown') as cost_category

        FROM {}
        WHERE cost_healthy_diet_ppp_usd IS NOT NULL
        ORDER BY country, year
    """.format(rel)
    )


def region_consistency_check(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Validate each country maps to exactly one region using SQL."""
    logger = logging.getLogger(__name__)

    inconsistent = rel.sql(
        """
        SELECT country, count(DISTINCT region) as region_count
        FROM {}
        GROUP BY country
        HAVING count(DISTINCT region) > 1
    """.format(rel)
    )

    count_inconsistent = inconsistent.aggregate("count(*)").fetchone()[0]
    if count_inconsistent > 0:
        examples = inconsistent.limit(5).df()
        logger.error(f"Found {count_inconsistent} countries with multiple regions")
        logger.error(f"Examples:\n{examples}")
        raise ValueError(f"{count_inconsistent} countries have inconsistent regions")

    logger.info("All countries have consistent regions")
    return rel


@click.command()
@click.argument("input_filepath", required=False)
@click.argument("output_filepath", required=False)
def main(input_filepath, output_filepath):
    """Run data processing pipeline entirely in DuckDB."""
    logger = logging.getLogger(__name__)
    load_dotenv()

    logger.info("Starting data processing pipeline")

    MY_BUCKET = os.getenv("MY_BUCKET")
    CHEMIN_FICHIER = os.getenv("CHEMIN_FICHIER")
    CHEMIN_PARQUET = None
    if MY_BUCKET and CHEMIN_FICHIER:
        CHEMIN_PARQUET = os.getenv(
            "CHEMIN_PARQUET", CHEMIN_FICHIER.replace(".csv", ".parquet")
        )
        make_parquet(MY_BUCKET, CHEMIN_FICHIER, CHEMIN_PARQUET)

    if not input_filepath and MY_BUCKET and CHEMIN_PARQUET:
        input_filepath = f"s3://{MY_BUCKET}/{CHEMIN_PARQUET}"
    if not input_filepath:
        raise click.UsageError(
            "Set MY_BUCKET and CHEMIN_FICHIER for S3 mode, or pass input parquet path."
        )
    if not output_filepath:
        if MY_BUCKET:
            output_filepath = os.getenv(
                "OUTPUT_PARQUET_PATH",
                f"s3://{MY_BUCKET}/processed/price_clean.parquet",
            )
        else:
            output_filepath = os.getenv(
                "OUTPUT_PARQUET_PATH", "/tmp/processed/price_clean.parquet"
            )

    # Full pipeline in DuckDB
    rel = load_data(input_filepath)
    rel = region_consistency_check(rel)

    # Overview
    overview = basic_overview(rel).df()
    logger.info(f"Data overview:\n{overview.to_string()}")

    # Clean pipeline
    rel_clean = clean_data(rel)

    # Save final result
    output_path = Path(output_filepath)
    if not str(output_filepath).startswith("s3://"):
        output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    if str(output_filepath).startswith("s3://"):
        configure_s3(con)
    out_sql = str(output_filepath).replace("'", "''")
    con.sql(f"COPY ({rel_clean}) TO '{out_sql}' (FORMAT PARQUET)")

    row_count = rel_clean.aggregate("count(*)").fetchone()[0]
    logger.info(f"Saved {row_count:,} rows to {output_path}")


if __name__ == "__main__":

    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    load_dotenv(find_dotenv())
    main()
