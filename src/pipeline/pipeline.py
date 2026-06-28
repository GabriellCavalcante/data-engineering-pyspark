# src/pipeline/pipeline.py
import logging

from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e grava no destino.
        """
        logger.info("Pipeline iniciado...")

        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

        pedidos_df = self.data_handler.load_pedidos(
            path=path_pedidos,
            compression=compression_pedidos,
            header=header_pedidos,
            sep=separator_pedidos,
        )

        logger.info("Abrindo o dataframe de pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]
        pagamentos_df = self.data_handler.load_pagamentos(path=path_pagamentos)

        logger.info("Aplicando as regras de negócio do relatório")
        relatorio_df = self.transformer.gerar_relatorio(pedidos_df, pagamentos_df)
        relatorio_df.show(100, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        path_output = config["paths"]["output"]
        logger.info(f"Obtido o path de saída: {path_output}")
        self.data_handler.write_parquet(df=relatorio_df, path=path_output)

        logger.info("Pipeline concluído com sucesso!")
