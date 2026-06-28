# src/session/spark_session.py
from pyspark.sql import SparkSession


class SparkSessionManager:
    """
    Gerencia a criação e o acesso à sessão Spark.
    """

    @staticmethod
    def get_spark_session(app_name: str = "alun-data-eng-pyspark-app") -> SparkSession:
        """
        Cria e retorna uma sessão Spark.

        :param app_name: Nome da aplicação Spark.
        :return: Instância da SparkSession.
        """
        return (
            SparkSession.builder.appName(app_name)
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate()
        )
