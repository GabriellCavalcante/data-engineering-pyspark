# tests/conftest.py
import os
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession compartilhada por toda a suíte de testes.

    A sessão é configurada para rodar 100% local, evitando interferência de
    SPARK_HOME, HADOOP_CONF_DIR, YARN ou HDFS configurados no ambiente.
    """

    os.environ.pop("SPARK_HOME", None)
    os.environ.pop("HADOOP_CONF_DIR", None)
    os.environ.pop("YARN_CONF_DIR", None)

    warehouse_dir = tempfile.TemporaryDirectory()
    spark_local_dir = tempfile.TemporaryDirectory()

    session = (
        SparkSession.builder
        .appName("test-pipeline-session")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", f"file://{warehouse_dir.name}")
        .config("spark.local.dir", spark_local_dir.name)
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("INFO")

    yield session

    session.stop()
    warehouse_dir.cleanup()
    spark_local_dir.cleanup()