import click
import logging
import os
import duckdb
import kagglehub
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from utils import configure_s3


def upload_to_s3(local_path: Path, bucket: str, s3_key: str):
    """Upload local file to S3 using DuckDB."""
    logger = logging.getLogger(__name__)
    con = duckdb.connect(database=":memory:")
    configure_s3(con)

    logger.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
    con.sql(
        f"COPY (SELECT * FROM read_csv_auto('{local_path}')) TO 's3://{bucket}/{s3_key}' (FORMAT CSV, HEADER)"
    )
    logger.info("Upload complete.")


@click.command()
@click.option(
    "--dataset",
    default="hassanjameelahmed/price-of-healthy-diet-clean",
    help="Kaggle dataset ID",
)
def main(dataset):
    logger = logging.getLogger(__name__)
    load_dotenv(find_dotenv())

    logger.info(f"Downloading {dataset} from Kaggle...")
    tmp_path = kagglehub.dataset_download(dataset)

    files = list(Path(tmp_path).glob("*.csv"))
    if not files:
        logger.error("No CSV found in Kaggle download")
        return

    bucket = os.getenv("MY_BUCKET")
    s3_key = os.getenv(
        "CHEMIN_FICHIER", "data/price_of_healthy_diet_clean.csv"
    )

    upload_to_s3(files[0], bucket, s3_key)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
