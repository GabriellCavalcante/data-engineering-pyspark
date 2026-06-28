import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dataeng_pyspark_data_pipeline.config.settings import carregar_config
from dataeng_pyspark_data_pipeline.io_utils.data_handler import DataHandler
from dataeng_pyspark_data_pipeline.pipeline.pipeline import Pipeline
from dataeng_pyspark_data_pipeline.processing.transformations import Transformation
from dataeng_pyspark_data_pipeline.session.spark_session import SparkSessionManager


def find_project_root() -> Path:
    """
    Encontra a raiz do projeto procurando pelo arquivo config/settings.yaml.

    Funciona tanto executando direto pela IDE quanto executando a partir
    da raiz do projeto.
    """
    candidates = [
        Path.cwd(),
        Path(__file__).resolve().parents[2],
    ]

    for candidate in candidates:
        if (candidate / "config" / "settings.yaml").exists():
            return candidate

    raise FileNotFoundError(
        "Não foi possível encontrar config/settings.yaml. "
        "Execute o projeto a partir da raiz ou abra a raiz do projeto na IDE."
    )


PROJECT_ROOT = find_project_root()
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

os.chdir(PROJECT_ROOT)


def configurar_logging() -> None:
    """Configura o logging para todo o projeto."""

    data_execucao = datetime.now().strftime("%Y%m%d")
    caminho_log = PROJECT_ROOT / f"dataeng-pyspark-{data_execucao}.log"

    with caminho_log.open("w", encoding="utf-8"):
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


def main() -> None:
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

    except Exception:
        logger.exception("FALHA CRÍTICA NO PIPELINE")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
