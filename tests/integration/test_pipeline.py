# tests/integration/test_pipeline.py
import gzip
import json
from decimal import Decimal
from unittest.mock import MagicMock

from pyspark.sql.types import (
    BooleanType, DecimalType, LongType, StringType,
    StructField, StructType,
)

from dataeng_pyspark_data_pipeline.io_utils.data_handler import DataHandler
from dataeng_pyspark_data_pipeline.pipeline.pipeline import Pipeline
from dataeng_pyspark_data_pipeline.processing.transformations import Transformation


SCHEMA_PEDIDOS = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", DecimalType(18, 2), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True),
])

SCHEMA_PAGAMENTOS = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DecimalType(18, 2), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", StringType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DecimalType(5, 4), True),
    ]), True),
])


def config_teste(output="/mock/output/"):
    return {
        "paths": {"pagamentos": "/mock/pagamentos/", "pedidos": "/mock/pedidos/", "output": output},
        "file_options": {"pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}},
    }


def _handler_mock(pedidos_df, pagamentos_df):
    """DataHandler falso que devolve DataFrames pré-definidos, sem ler disco."""
    handler = MagicMock(spec=DataHandler)
    handler.load_pedidos.return_value = pedidos_df
    handler.load_pagamentos.return_value = pagamentos_df
    return handler


class TestPipelineOrquestracao:

    def test_le_pedidos_e_pagamentos_com_paths_da_config(self, spark):
        pedidos = spark.createDataFrame([], SCHEMA_PEDIDOS)
        pagamentos = spark.createDataFrame([], SCHEMA_PAGAMENTOS)
        handler = _handler_mock(pedidos, pagamentos)
        Pipeline(handler, Transformation()).run(config_teste())
        handler.load_pedidos.assert_called_once_with(
            path="/mock/pedidos/", compression="gzip", header=True, sep=";",
        )
        handler.load_pagamentos.assert_called_once_with(path="/mock/pagamentos/")

    def test_grava_no_path_de_output(self, spark):
        pedidos = spark.createDataFrame([], SCHEMA_PEDIDOS)
        pagamentos = spark.createDataFrame([], SCHEMA_PAGAMENTOS)
        handler = _handler_mock(pedidos, pagamentos)
        Pipeline(handler, Transformation()).run(config_teste())
        assert handler.write_parquet.call_args.kwargs["path"] == "/mock/output/"


class TestPipelineEndToEnd:

    def test_pipeline_completo_gera_parquet_valido(self, spark, tmp_path):
        pedidos_lines = [
            "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
            "p1;TV;100.00;2;2025-01-01T10:00:00;SP;1",
            "p1;CABO;50.00;1;2025-01-01T10:00:00;SP;1",
            "p2;PC;300.00;1;2024-01-01T10:00:00;RJ;2",
        ]
        pagamentos = [
            {"id_pedido": "p1", "forma_pagamento": "pix", "valor_pagamento": 250.0,
             "status": False, "data_processamento": "2025-01-01",
             "avaliacao_fraude": {"fraude": False, "score": 0.1}},
            {"id_pedido": "p2", "forma_pagamento": "boleto", "valor_pagamento": 300.0,
             "status": False, "data_processamento": "2024-01-01",
             "avaliacao_fraude": {"fraude": False, "score": 0.2}},
        ]
        pedidos_path = tmp_path / "pedidos.csv.gz"
        pagamentos_path = tmp_path / "pagamentos.json.gz"
        with gzip.open(pedidos_path, "wt", encoding="utf-8") as f:
            f.write("\n".join(pedidos_lines))
        with gzip.open(pagamentos_path, "wt", encoding="utf-8") as f:
            for p in pagamentos:
                f.write(json.dumps(p) + "\n")
        output_path = str(tmp_path / "output")
        config = config_teste(output_path)
        config["paths"]["pedidos"] = str(pedidos_path)
        config["paths"]["pagamentos"] = str(pagamentos_path)

        Pipeline(DataHandler(spark), Transformation()).run(config)
        resultado = spark.read.parquet(output_path)
        assert resultado.count() == 1
        assert resultado.collect()[0].valor_total_pedido == Decimal("250.00")
