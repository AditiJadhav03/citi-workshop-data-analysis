"""EKS entry point for the ACME team analytics pipeline.

The Helm chart mounts a single script from a ConfigMap, but the pipeline is a
twenty module package. This launcher bridges the two: it fetches the packaged
pipeline from S3 with boto3, puts the archive on sys.path (Python imports
directly from a zip), and hands over to the same data_pipeline.main entry point
used locally. Nothing about the pipeline changes for cloud execution.

boto3 is used rather than the AWS CLI because the base image does not ship the
CLI, and boto3 is installed from requirements.txt before this script runs.
"""

import logging
import os
import sys
import tempfile
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("job")

BUCKET = os.environ.get("PIPELINE_BUCKET", "your-pipeline-bucket")
PIPELINE_KEY = os.environ.get("PIPELINE_KEY", "pipeline/pipeline.zip")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "acme-data/raw")
WORK = Path(tempfile.gettempdir()) / "acme"


def diagnostics() -> None:
    """Log the execution environment before anything can fail silently."""
    log.info("python      : %s", sys.version.split()[0])
    log.info("cwd         : %s", os.getcwd())
    log.info("user        : uid=%s", os.getuid())
    log.info("bucket      : %s", BUCKET)
    for module in ("boto3", "pyspark", "pandas"):
        try:
            mod = __import__(module)
            log.info("%-11s : %s", module, getattr(mod, "__version__", "present"))
        except ImportError as exc:
            log.error("%-11s : MISSING (%s)", module, exc)


def s3_client():
    """Return an S3 client, using whatever credentials the pod provides."""
    import boto3

    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-south-1"))


def fetch_pipeline(s3) -> Path:
    """Download the packaged pipeline and place it on the import path."""
    WORK.mkdir(parents=True, exist_ok=True)
    archive = WORK / "pipeline.zip"
    log.info("Downloading s3://%s/%s", BUCKET, PIPELINE_KEY)
    s3.download_file(BUCKET, PIPELINE_KEY, str(archive))
    sys.path.insert(0, str(archive))
    log.info("Pipeline package on sys.path (%.1f KB)", archive.stat().st_size / 1024)
    return archive


def fetch_raw_data(s3) -> Path:
    """Download the source exports the pipeline reads from."""
    raw = WORK / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative = key[len(RAW_PREFIX):].lstrip("/")
            if not relative:
                continue
            target = raw / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, key, str(target))
            log.info("  %s (%.1f MB)", relative, obj["Size"] / 1e6)
            count += 1

    if count == 0:
        raise RuntimeError(f"No source files under s3://{BUCKET}/{RAW_PREFIX}")
    log.info("Fetched %d source files", count)
    return raw


def configure(raw: Path) -> None:
    """Set the environment the pipeline reads its configuration from.

    The data lake lives in S3, so IS_LOCAL is false and every layer path
    resolves to an s3a:// URI inside config.lake_root(). Ingestion is forced
    through the file route: the PostgreSQL path is exercised locally where the
    database credentials are injected, while this deployment demonstrates the
    S3 route and Spark on Kubernetes.
    """
    os.environ.update(
        {
            "IS_LOCAL": "false",
            "ENGINE": "spark",
            "LOG_FORMAT": "text",
            "DATA_LAKE_BUCKET": BUCKET,
            "DATA_LAKE_PREFIX": "acme-data-lake",
            "RAW_DIR": str(raw),
            "INGEST_MODE_OVERRIDE": "file",
            "SPARK_MASTER": "local[*]",
            "SPARK_DRIVER_MEMORY": os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
            "SPARK_SHUFFLE_PARTITIONS": "4",
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "unused"),
            "POSTGRES_NAME": os.environ.get("POSTGRES_NAME", "unused"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "unused"),
            "POSTGRES_PASS": os.environ.get("POSTGRES_PASS", "unused"),
        }
    )
    os.environ.setdefault("AWS_REGION", "ap-south-1")


def main() -> int:
    """Fetch, configure and run the pipeline."""
    log.info("=" * 70)
    log.info("ACME team analytics pipeline - EKS execution")
    log.info("=" * 70)
    diagnostics()

    try:
        s3 = s3_client()
        fetch_pipeline(s3)
        raw = fetch_raw_data(s3)
    except Exception:
        log.exception("Setup failed before the pipeline could start")
        return 2

    configure(raw)

    from data_pipeline.main import main as pipeline_main

    log.info("Handing over to data_pipeline.main")
    exit_code = pipeline_main(["--log-format", "text"])

    log.info("Pipeline exited with code %s", exit_code)
    if exit_code == 0:
        log.info("Gold written to s3://%s/acme-data-lake/gold/", BUCKET)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
