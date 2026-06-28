# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from pipeline.pipeline import Pipeline
from processing.transformations import Transformation
import logging
from datetime import datetime
import os


# Crie a configuração do logging
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
        force=True,  # força substituir handlers já existentes
    )

    logging.info("Logging configurado.")
    logging.info("Arquivo de log em: %s", caminho_log)


def main():
    logger = logging.getLogger(__name__)

    config = carregar_config()
    app_name = config["spark"]["app_name"]

    spark = None  # Inicializa como None para segurança no finally
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)

        data_handler = DataHandler(spark)
        transformer = Transformation()

        pipeline = Pipeline(data_handler, transformer)
        pipeline.run(config=config)
        logger.info("[main] relatório executado com sucesso!!!")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA NO PIPELINE: {e}")
        # Aqui poderíamos adicionar envio de notificação (Slack, Email, PagerDuty)

    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    configurar_logging()
    main()
