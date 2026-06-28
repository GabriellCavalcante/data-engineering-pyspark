# src/processing/transformations.py
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logger = logging.getLogger(__name__)


class Transformation:
    """Classe que contém as transformações e regras de negócio da aplicação."""

    def gerar_relatorio(
        self, pedidos_df: DataFrame, pagamentos_df: DataFrame
    ) -> DataFrame:
        """Gera o relatório de pedidos 2025 com pagamento recusado e legítimo."""
        try:
            logger.info("Aplicando regras de negócio do relatório.")

            data_column = (
                "data_criacao"
                if "data_criacao" in pedidos_df.columns
                else "data_pedido"
            )

            pedidos = (
                pedidos_df.withColumn(
                    "data_pedido",
                    F.coalesce(
                        F.try_to_timestamp(
                            F.col(data_column),
                            F.lit("yyyy-MM-dd'T'HH:mm:ss"),
                        ),
                        F.try_to_timestamp(
                            F.col(data_column),
                            F.lit("yyyy-MM-dd HH:mm:ss"),
                        ),
                        F.try_to_timestamp(
                            F.col(data_column),
                            F.lit("yyyy-MM-dd"),
                        ),
                    ),
                )
                .withColumn("uf", F.upper(F.trim("uf")))
                .filter(
                    F.col("id_pedido").isNotNull() & F.col("data_pedido").isNotNull()
                )
                .filter(
                    (F.col("data_pedido") >= F.lit("2025-01-01").cast("timestamp"))
                    & (F.col("data_pedido") < F.lit("2026-01-01").cast("timestamp"))
                )
                .withColumn(
                    "valor_item",
                    (F.col("valor_unitario") * F.col("quantidade")).cast(
                        DecimalType(20, 2)
                    ),
                )
                .groupBy("id_pedido", "uf", "data_pedido")
                .agg(
                    F.sum("valor_item")
                    .cast(DecimalType(20, 2))
                    .alias("valor_total_pedido")
                )
            )

            pagamentos = (
                pagamentos_df.filter(F.col("id_pedido").isNotNull())
                .filter(
                    (F.col("status") == F.lit(False))
                    & (F.col("avaliacao_fraude.fraude") == F.lit(False))
                )
                .select(
                    "id_pedido",
                    F.upper(F.trim("forma_pagamento")).alias("forma_pagamento"),
                )
            )

            return (
                pedidos.join(pagamentos, on="id_pedido", how="inner")
                .select(
                    "id_pedido",
                    "uf",
                    "forma_pagamento",
                    "valor_total_pedido",
                    "data_pedido",
                )
                .orderBy("uf", "forma_pagamento", "data_pedido")
            )

        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {e}")
            raise e
