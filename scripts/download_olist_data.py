"""Script to prepare and download Olist datasets."""

import logging
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.data_platform.config import settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("download_olist_data")

OLIST_FILES = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]


def check_existing_files(raw_dir: Path) -> list[str]:
    """Check which expected Olist files already exist in data/raw."""
    present = []
    for filename in OLIST_FILES:
        filepath = raw_dir / filename
        if filepath.exists() and filepath.stat().st_size > 0:
            present.append(filename)
    return present


def main() -> None:
    raw_dir = settings.raw_path
    raw_dir.mkdir(parents=True, exist_ok=True)

    present = check_existing_files(raw_dir)
    missing = [f for f in OLIST_FILES if f not in present]

    logger.info("Found %d/%d Olist datasets in %s:", len(present), len(OLIST_FILES), raw_dir)
    for p in present:
        size_mb = (raw_dir / p).stat().st_size / (1024 * 1024)
        logger.info("  ✓ %s (%.2f MB)", p, size_mb)

    if missing:
        logger.info("Missing datasets (%d):", len(missing))
        for m in missing:
            logger.info("  ✗ %s", m)

        kaggle_user = os.getenv("KAGGLE_USERNAME")
        kaggle_key = os.getenv("KAGGLE_KEY")

        if kaggle_user and kaggle_key:
            logger.info("Kaggle credentials found. Attempting download via Kaggle API...")
            try:
                import kaggle  # type: ignore

                kaggle.api.dataset_download_files(
                    "olistbr/brazilian-ecommerce",
                    path=str(raw_dir),
                    unzip=True,
                )
                logger.info("Download and extraction completed.")
            except ImportError:
                logger.warning(
                    "kaggle package is not installed. Run 'pip install kaggle' to enable automated download."
                )
            except Exception as e:
                logger.error("Failed to download dataset from Kaggle: %s", e)
        else:
            logger.info(
                "To automatically download remaining datasets, configure KAGGLE_USERNAME and KAGGLE_KEY in .env, "
                "or place the CSV files manually in '%s'.",
                raw_dir,
            )


if __name__ == "__main__":
    main()
