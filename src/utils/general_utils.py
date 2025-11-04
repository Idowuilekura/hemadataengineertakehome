from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple
import datetime

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

LOG_LEVEL = "INFO"


class SeparatorFormatter(logging.Formatter):
    """Custom formatter that:
    - Appends a separator line after every log record.
    - Includes the function name for ERROR and higher logs.
    """

    SEP = "-" * 30

    def format(self, record: logging.LogRecord) -> str:
        if record.levelno >= logging.ERROR:
            fmt = "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
        else:
            fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        base = logging.Formatter(fmt)
        rendered = base.format(record)
        return f"{rendered}\n{self.SEP}"


def get_logger(name: str = "hema_etl", level: str = LOG_LEVEL) -> logging.Logger:
    """Create (or retrieve) a configured logger.

    Parameters
    ----------
    name : str, optional
        Logger name; use module or application name to differentiate streams.
    level : str, optional
        Logging level as a string (e.g., "INFO", "DEBUG", "WARNING").

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(SeparatorFormatter())
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger


LOGGER = get_logger(__name__, LOG_LEVEL)


def build_spark(app_name: str = "hema_etl_notebook",
                remote_url: Optional[str] = None,
                local: bool = True) -> SparkSession:
    """Create a SparkSession either locally or via remote mode."""
    try:
        if local:
            LOGGER.info("Creating local SparkSession with app_name=%s", app_name)
            return SparkSession.builder.appName(app_name).getOrCreate()
        else:
            if not remote_url:
                raise ValueError("remote_url must be provided when local=False")
            LOGGER.info("Connecting to remote SparkSession at %s (app_name=%s)", remote_url, app_name)
            return SparkSession.builder.remote(remote_url).appName(app_name).getOrCreate()
    except Exception as exc:
        LOGGER.exception("Failed to create SparkSession: %s", exc)
        raise

def add_ingestion_metadata(df: DataFrame, file_path: str) -> DataFrame:
    """
    Add standard ingestion metadata columns to a DataFrame.

    Columns added
    --------------
    - file_path : str
        The path or identifier of the file/source from which the DataFrame originated.
    - execution_datetime : str
        The UTC timestamp when this DataFrame was processed.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame to enrich with metadata.
    file_path : str
        The path to the source file being processed.

    Returns
    -------
    pyspark.sql.DataFrame
        The input DataFrame with two additional metadata columns.
    """
    execution_ts = datetime.datetime.utcnow().isoformat()
    return (
        df.withColumn("file_path", F.lit(file_path))
          .withColumn("execution_datetime", F.lit(execution_ts))
    )