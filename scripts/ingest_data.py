"""
Ingest Script — Kaggle download + GCS upload (Cloud Run Job)
=============================================================
Downloads the stock market dataset from Kaggle and uploads it to GCS.
Idempotent: checks if files already exist in GCS before downloading.

Environment variables (injected by Cloud Run / Secret Manager):
  KAGGLE_USERNAME   — Kaggle account username
  KAGGLE_KEY        — Kaggle API key
  GCS_BUCKET        — GCS bucket name (default: dezoomcampstore)
  GCP_PROJECT_ID    — GCP project (default: dezoomcamp-486216)
"""

import os
import sys
import zipfile
import tempfile
from pathlib import Path

from google.cloud import storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAGGLE_DATASET = "jacksoncrow/stock-market-dataset"
GCS_BUCKET     = os.environ.get("GCS_BUCKET", "dezoomcampstore")
GCS_RAW_PREFIX = "security-market-raw-data"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gcs_prefix_has_files(client: storage.Client, bucket_name: str, prefix: str) -> bool:
    """Return True if any blobs exist under the given prefix."""
    blobs = list(client.list_blobs(bucket_name, prefix=prefix, max_results=1))
    return len(blobs) > 0


def upload_directory(client: storage.Client, bucket_name: str, local_dir: Path, gcs_prefix: str) -> None:
    """Recursively upload a local directory to GCS."""
    bucket = client.bucket(bucket_name)
    files  = list(local_dir.rglob("*"))
    total  = sum(1 for f in files if f.is_file())
    print(f"  Uploading {total} files from {local_dir} → gs://{bucket_name}/{gcs_prefix}/")
    uploaded = 0
    for fpath in files:
        if not fpath.is_file():
            continue
        blob_name = f"{gcs_prefix}/{fpath.relative_to(local_dir)}"
        bucket.blob(blob_name).upload_from_filename(str(fpath))
        uploaded += 1
        if uploaded % 500 == 0:
            print(f"    {uploaded}/{total} uploaded...")
    print(f"  Done: {uploaded} files uploaded.")


def upload_file(client: storage.Client, bucket_name: str, local_path: Path, gcs_path: str) -> None:
    client.bucket(bucket_name).blob(gcs_path).upload_from_filename(str(local_path))
    print(f"  Uploaded {local_path.name} → gs://{bucket_name}/{gcs_path}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    gcs_client = storage.Client()

    print(f"\n{'='*60}")
    print(f"  Stock Market Pipeline — Ingest to GCS")
    print(f"  Bucket: gs://{GCS_BUCKET}/{GCS_RAW_PREFIX}")
    print(f"{'='*60}\n")

    # ── Idempotency check ────────────────────────────────────────────────────
    stocks_prefix = f"{GCS_RAW_PREFIX}/stocks"
    if gcs_prefix_has_files(gcs_client, GCS_BUCKET, stocks_prefix):
        print("Data already present in GCS — skipping download and upload.")
        print(f"  gs://{GCS_BUCKET}/{stocks_prefix}/ already has files.")
        print("To force a re-download, delete the GCS prefix first.")
        sys.exit(0)

    # ── Kaggle download ──────────────────────────────────────────────────────
    # The kaggle library reads KAGGLE_USERNAME / KAGGLE_KEY from env vars.
    import kaggle  # noqa: PLC0415 — imported here to defer credential check

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        print(f"Downloading {KAGGLE_DATASET} to {tmp}...")
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            KAGGLE_DATASET,
            path=str(tmp),
            unzip=False,  # we unzip manually to control the path
        )

        # Find and unzip the downloaded archive
        zips = list(tmp.glob("*.zip"))
        if not zips:
            raise FileNotFoundError(f"No zip file found in {tmp} after Kaggle download.")

        zip_path = zips[0]
        print(f"Extracting {zip_path.name}...")
        extract_dir = tmp / "extracted"
        extract_dir.mkdir()
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        # ── Upload to GCS ────────────────────────────────────────────────────
        print("\nUploading to GCS...")

        stocks_dir = extract_dir / "stocks"
        etfs_dir   = extract_dir / "etfs"
        meta_csv   = extract_dir / "symbols_valid_meta.csv"

        if stocks_dir.exists():
            upload_directory(gcs_client, GCS_BUCKET, stocks_dir, f"{GCS_RAW_PREFIX}/stocks")
        else:
            print(f"  WARNING: {stocks_dir} not found in archive.")

        if etfs_dir.exists():
            upload_directory(gcs_client, GCS_BUCKET, etfs_dir, f"{GCS_RAW_PREFIX}/etfs")
        else:
            print(f"  WARNING: {etfs_dir} not found in archive.")

        if meta_csv.exists():
            upload_file(gcs_client, GCS_BUCKET, meta_csv, f"{GCS_RAW_PREFIX}/symbols_valid_meta.csv")
        else:
            print(f"  WARNING: {meta_csv} not found in archive.")

    print("\nIngest complete.")


if __name__ == "__main__":
    main()
