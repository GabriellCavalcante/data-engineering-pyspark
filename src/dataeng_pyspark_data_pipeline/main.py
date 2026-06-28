import logging
import os
from datetime import datetime

from dataeng_pyspark_data_pipeline.config.settings import carregar_config
from dataeng_pyspark_data_pipeline.io_utils.data_handler import DataHandler
from dataeng_pyspark_data_pipeline.pipeline.pipeline import Pipeline
from dataeng_pyspark_data_pipeline.processing.transformations import Transformation
from dataeng_pyspark_data_pipeline.session.spark_session import SparkSessionManager


def configurar_logging():
    """Configura o logging para todo o projeto."""

    data_execucao = datetime.now().strftime("%Y%m%d")
    caminho_log = os.path.abspath(f"dataeng-pyspark-{data_execucao}.log")

    with open(caminho_log, "w", encoding="utf-8"):
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(caminho_log, mode="a", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )

    logging.info("Logging configurado.")
    logging.info("Arquivo de log em: %s", caminho_log)


def main():
    configurar_logging()

    logger = logging.getLogger(__name__)

    config = carregar_config()
    app_name = config["spark"]["app_name"]

    spark = None

    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)

        data_handler = DataHandler(spark)
        transformer = Transformation()

        pipeline = Pipeline(data_handler, transformer)
        pipeline.run(config=config)

        logger.info("[main] relatório executado com sucesso!!!")

    except Exception as e:
        logging.error("FALHA CRÍTICA NO PIPELINE: %s", e)

    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
