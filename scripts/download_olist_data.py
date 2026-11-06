"""Script to prepare, audit and download Olist datasets (Manual & Automated Kaggle options)."""

from __future__ import annotations

import argparse
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


def check_existing_files(raw_dir: Path) -> tuple[list[str], list[str]]:
    """Check which expected Olist files already exist in data/raw."""
    present = []
    missing = []
    for filename in OLIST_FILES:
        filepath = raw_dir / filename
        if filepath.exists() and filepath.stat().st_size > 0:
            present.append(filename)
        else:
            missing.append(filename)
    return present, missing


def print_manual_instructions(raw_dir: Path, missing: list[str]) -> None:
    """Prints step-by-step instructions for manual download."""
    print("\n" + "=" * 70)
    print("[GUIA] OPCION B: DESCARGA MANUAL DEL DATASET DE OLIST (KAGGLE)")
    print("=" * 70)
    print("1. Accede al dataset oficial en Kaggle:")
    print("   Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
    print("2. Haz clic en el boton 'Download' (descargara un archivo .zip de ~45 MB).")
    print("3. Descomprime los archivos CSV y colocalos en la carpeta:")
    print(f"   Directorio: {raw_dir.resolve()}")
    if missing:
        print("\nArchivos pendientes por copiar:")
        for m in missing:
            print(f"   - {m}")
    print("=" * 70 + "\n")


def download_automated_kaggle(raw_dir: Path) -> bool:
    """Attempts automated download using Kaggle API credentials."""
    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_user or not kaggle_key:
        logger.warning("Credenciales de Kaggle no encontradas en variables de entorno o archivo .env.")
        logger.info("Configura KAGGLE_USERNAME y KAGGLE_KEY en tu archivo .env para descarga automatica.")
        return False

    logger.info("Credenciales detectadas para usuario '%s'. Iniciando descarga via Kaggle API...", kaggle_user)
    try:
        import kaggle  # type: ignore

        kaggle.api.dataset_download_files(
            "olistbr/brazilian-ecommerce",
            path=str(raw_dir),
            unzip=True,
        )
        logger.info("[SUCCESS] Descarga y extraccion completadas exitosamente en %s.", raw_dir)
        return True
    except ImportError:
        logger.error("El paquete 'kaggle' no esta instalado. Ejecuta: pip install kaggle")
        return False
    except Exception as exc:
        logger.error("Error al descargar el dataset desde Kaggle API: %s", exc)
        return False


def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass
    parser = argparse.ArgumentParser(
        description="Gestor de descarga y verificación de datasets Olist (Manual o Automatizado)."
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "manual", "status"],
        default="status",
        help="Modo de ejecución: 'status' (audita), 'auto' (descarga con Kaggle API), 'manual' (muestra guía).",
    )
    args = parser.parse_args()

    raw_dir = settings.raw_path
    raw_dir.mkdir(parents=True, exist_ok=True)

    present, missing = check_existing_files(raw_dir)

    logger.info("Auditoría de Datasets Olist en '%s':", raw_dir)
    logger.info("  Archivos presentes: %d/%d", len(present), len(OLIST_FILES))
    for p in present:
        size_mb = (raw_dir / p).stat().st_size / (1024 * 1024)
        logger.info("    ✓ %s (%.2f MB)", p, size_mb)

    if missing:
        logger.info("  Archivos faltantes: %d", len(missing))
        for m in missing:
            logger.info("    ✗ %s", m)

    if args.mode == "manual":
        print_manual_instructions(raw_dir, missing)
    elif args.mode == "auto":
        if missing:
            success = download_automated_kaggle(raw_dir)
            if not success:
                print_manual_instructions(raw_dir, missing)
        else:
            logger.info("Todos los archivos ya están presentes. No se requiere descarga adicional.")
    else:  # status mode
        if missing:
            logger.info(
                "\nTienes dos opciones para completar los datasets faltantes:\n"
                "  1. Automatizada: python scripts/download_olist_data.py --mode auto (requiere credenciales Kaggle)\n"
                "  2. Manual: python scripts/download_olist_data.py --mode manual (instrucciones de descarga directa)"
            )
        else:
            logger.info("✅ Todos los archivos de Olist están verificados e íntegros.")


if __name__ == "__main__":
    main()
