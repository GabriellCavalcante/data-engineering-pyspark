# src/io_utils/data_handler.py
import logging

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", DecimalType(18, 2), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", StringType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pagamentos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", DecimalType(18, 2), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", StringType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", DecimalType(5, 4), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        try:
            return (
                self.spark.read.format("csv")
                .option("compression", compression)
                .option("header", header)
                .option("sep", sep)
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .schema(self._get_schema_pedidos())
                .load(path)
            )
        except AnalysisException as e:
            logger.error(f"Erro ao ler pedidos: {e}")
            raise e
        except Py4JJavaError as e:
            logger.critical(f"Erro crítico na JVM ao ler pedidos: {e}")
            raise e
        except Exception as e:
            logger.error(f"Erro desconhecido ao ler pedidos: {e}")
            raise e

    def load_pagamentos(self, path: str) -> DataFrame:
        try:
            return (
                self.spark.read.format("json")
                .option("mode", "FAILFAST")
                .schema(self._get_schema_pagamentos())
                .load(path)
            )
        except AnalysisException as e:
            logger.error(f"Erro ao ler pagamentos: {e}")
            raise e
        except Py4JJavaError as e:
            logger.critical(f"Erro crítico na JVM ao ler pagamentos: {e}")
            raise e
        except Exception as e:
            logger.error(f"Erro desconhecido ao ler pagamentos: {e}")
            raise e

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)
        logger.info(f"Dados salvos com sucesso em: {path}")
