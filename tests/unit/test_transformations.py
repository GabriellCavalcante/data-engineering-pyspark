# tests/unit/test_transformations.py
from decimal import Decimal

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dataeng_pyspark_data_pipeline.processing.transformations import Transformation

SCHEMA_PEDIDOS = StructType(
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

SCHEMA_PAGAMENTOS = StructType(
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


class TestTransformation:

    def test_relatorio_filtra_2025_status_false_e_fraude_false(self, spark):
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", Decimal("100.00"), 2, "2025-01-01T10:00:00", " sp ", 1),
                ("p2", "PC", Decimal("300.00"), 1, "2024-01-01T10:00:00", "RJ", 2),
            ],
            SCHEMA_PEDIDOS,
        )
        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    " pix ",
                    Decimal("200.00"),
                    False,
                    "2025-01-01",
                    (False, Decimal("0.1000")),
                ),
                (
                    "p2",
                    "boleto",
                    Decimal("300.00"),
                    False,
                    "2024-01-01",
                    (False, Decimal("0.2000")),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )
        resultado = Transformation().gerar_relatorio(pedidos, pagamentos).collect()
        assert len(resultado) == 1
        assert resultado[0].id_pedido == "p1"
        assert resultado[0].uf == "SP"
        assert resultado[0].forma_pagamento == "PIX"
        assert resultado[0].valor_total_pedido == Decimal("200.00")

    def test_soma_multiplos_itens_do_mesmo_pedido(self, spark):
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", Decimal("100.00"), 2, "2025-02-01", "SP", 1),
                ("p1", "CABO", Decimal("50.00"), 1, "2025-02-01", "SP", 1),
            ],
            SCHEMA_PEDIDOS,
        )
        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    Decimal("250.00"),
                    False,
                    "2025-02-01",
                    (False, Decimal("0.1000")),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )
        linha = Transformation().gerar_relatorio(pedidos, pagamentos).collect()[0]
        assert linha.valor_total_pedido == Decimal("250.00")

    def test_exclui_pagamento_aprovado_ou_fraudulento(self, spark):
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", Decimal("100.00"), 1, "2025-01-01", "SP", 1),
                ("p2", "PC", Decimal("100.00"), 1, "2025-01-01", "SP", 2),
            ],
            SCHEMA_PEDIDOS,
        )
        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "pix",
                    Decimal("100.00"),
                    True,
                    "2025-01-01",
                    (False, Decimal("0.1000")),
                ),
                (
                    "p2",
                    "pix",
                    Decimal("100.00"),
                    False,
                    "2025-01-01",
                    (True, Decimal("0.9000")),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )
        assert Transformation().gerar_relatorio(pedidos, pagamentos).count() == 0
