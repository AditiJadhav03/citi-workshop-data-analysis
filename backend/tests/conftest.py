"""Shared pytest fixtures.

A single Spark session is reused across the whole test session, because
starting a JVM per test would make the suite unusably slow.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """A minimal local Spark session configured like the pipeline's own."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("pipeline-tests")
        .master("local[2]")
        # Matches the pipeline: a failed cast yields null rather than raising,
        # which is what lets validation detect and quarantine the row.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def apply_transform(spark):
    """Apply a Column-returning transform to a list of inputs.

    Returns the results as plain Python values, so tests read as a simple
    input-to-expected mapping rather than as dataframe plumbing.
    """
    from pyspark.sql import functions as F

    def _apply(transform, values, column="value"):
        rows = [(v,) for v in values]
        # An explicit schema avoids type inference, which cannot resolve a
        # column that is entirely null and behaves inconsistently across
        # Spark versions.
        frame = spark.createDataFrame(rows, f"{column} string")
        return [row[0] for row in frame.select(transform(F.col(column))).collect()]

    return _apply
