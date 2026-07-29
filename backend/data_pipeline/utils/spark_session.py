"""Spark session factory.

The session is created exactly once per run, from the settings declared in
``config``. Stages receive the handle through ``PipelineContext`` and must never
call ``getOrCreate`` themselves, so that packages, S3 credentials and the ANSI
setting are guaranteed identical everywhere.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import config
from .logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_LOGGER = get_logger(__name__)


def create_spark_session(app_name: str | None = None) -> "SparkSession":
    """Build (or fetch) the Spark session for this run.

    Args:
        app_name: Overrides the configured application name. Useful in tests.

    Returns:
        A configured SparkSession.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name or config.SPARK_APP_NAME)

    if config.SPARK_MASTER:
        builder = builder.master(config.SPARK_MASTER)

    for key, value in config.spark_conf().items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    _LOGGER.info(
        "Spark session ready",
        extra={
            "spark_version": session.version,
            "master": session.sparkContext.master,
            "ansi_enabled": session.conf.get("spark.sql.ansi.enabled"),
            "shuffle_partitions": session.conf.get("spark.sql.shuffle.partitions"),
        },
    )
    return session


def stop_spark_session(session: "SparkSession | None") -> None:
    """Stop the session if one is running."""
    if session is not None:
        session.stop()
        _LOGGER.info("Spark session stopped")