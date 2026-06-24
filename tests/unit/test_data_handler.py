# tests/unit/test_data_handler.py
import gzip
import json
import os
from decimal import Decimal

import pytest
from pyspark.sql.types import BooleanType, DecimalType, LongType, StringType, StructField, StructType

from io_utils.data_handler import DataHandler


@pytest.fixture
def arquivo_pagamentos_gz(tmp_path):
    """Arquivo JSON gzipado com pagamentos de exemplo."""
    pagamentos = [
        {"id_pedido": "p1", "forma_pagamento": "pix", "valor_pagamento": 100.0,
         "status": False, "data_processamento": "2025-01-01",
         "avaliacao_fraude": {"fraude": False, "score": 0.1}},
        {"id_pedido": "p2", "forma_pagamento": "boleto", "valor_pagamento": 200.0,
         "status": True, "data_processamento": "2025-01-02",
         "avaliacao_fraude": {"fraude": False, "score": 0.2}},
    ]
    gz_path = tmp_path / "pagamentos.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for p in pagamentos:
            f.write(json.dumps(p) + "\n")
    return str(gz_path)


@pytest.fixture
def arquivo_pedidos_gz(tmp_path):
    """Arquivo CSV gzipado com três pedidos de exemplo."""
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "p1;TV;100.00;2;2025-01-01T10:00:00;SP;1",
        "p1;CABO;50.00;1;2025-01-01T10:00:00;SP;1",
        "p2;PC;300.00;1;2024-01-02T11:00:00;RJ;2",
    ]
    gz_path = tmp_path / "pedidos.csv.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    return str(gz_path)


class TestLoadPagamentos:

    def test_le_json_gz_e_retorna_dataframe(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(arquivo_pagamentos_gz)
        assert df.count() == 2

    def test_schema_pagamentos_aplica_tipos_corretos(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(arquivo_pagamentos_gz)
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["status"], BooleanType)
        assert isinstance(tipos["valor_pagamento"], DecimalType)


class TestLoadPedidos:

    def test_le_csv_gz_com_separador_ponto_e_virgula(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz, compression="gzip", header=True, sep=";",
        )
        assert df.count() == 3

    def test_schema_pedidos_tem_tipos_corretos(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz, compression="gzip", header=True, sep=";",
        )
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["id_pedido"], StringType)
        assert isinstance(tipos["quantidade"], LongType)


class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
        """Verificar só a criação do diretório não basta: relemos para garantir integridade."""
        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("valor_total_pedido", DecimalType(20, 2), True),
        ])
        df = spark.createDataFrame([("p1", Decimal("250.00"))], schema)
        output_path = str(tmp_path / "saida_parquet")
        DataHandler(spark).write_parquet(df, output_path)
        assert os.path.exists(output_path)
        assert spark.read.parquet(output_path).count() == 1
