# Data Engineering PySpark Pipeline

Projeto de engenharia de dados desenvolvido em Python com PySpark para gerar um relatório analítico de pedidos de venda. O pipeline lê dados de pedidos em CSV compactado e dados de pagamentos em JSON, aplica regras de negócio, cruza as informações e grava o resultado final em formato Parquet.

## Objetivo do projeto

O objetivo do projeto é gerar um relatório contendo pedidos que atendem às seguintes regras de negócio:

- pedidos realizados no ano de 2025;
- pagamentos recusados, isto é, `status = false`;
- pedidos classificados como legítimos pela avaliação de fraude, isto é, `avaliacao_fraude.fraude = false`;
- cálculo do valor total do pedido a partir dos itens, usando `valor_unitario * quantidade`;
- gravação do resultado final em Parquet.

O resultado final contém os seguintes campos:

```text
id_pedido
uf
forma_pagamento
valor_total_pedido
data_pedido
```

A ordenação final é feita por:

```text
uf, forma_pagamento, data_pedido
```

## Tecnologias utilizadas

- Python 3.10 ou superior
- PySpark 4.1.1
- Java 17
- PyYAML
- Pytest
- Pytest-cov
- Ruff
- Black
- Setuptools / Build

## Pré-requisitos

Antes de executar o projeto, a estação de trabalho precisa ter:

```text
Python >= 3.10.12
Java 17
pip
venv
```

Para conferir o Java:

```bash
java -version
```

O esperado é uma versão parecida com:

```text
openjdk version "17.x.x"
```

> Observação importante: este projeto usa o PySpark instalado via `pip` dentro da `.venv`. Por isso, não é necessário configurar `SPARK_HOME`. Caso exista um `SPARK_HOME` apontando para outra instalação do Spark, pode ocorrer mistura de versões entre o PySpark da `.venv` e um Spark externo.

## Estrutura do projeto

```text
data-engineering-pyspark/
├── config/
│   └── settings.yaml
├── data/
│   ├── input/
│   │   ├── dataset-json-pagamentos/
│   │   └── datasets-csv-pedidos/
│   └── output/
├── src/
│   └── dataeng_pyspark_data_pipeline/
│       ├── __init__.py
│       ├── main.py
│       ├── config/
│       │   └── settings.py
│       ├── io_utils/
│       │   └── data_handler.py
│       ├── pipeline/
│       │   └── pipeline.py
│       ├── processing/
│       │   └── transformations.py
│       └── session/
│           └── spark_session.py
├── tests/
│   ├── conftest.py
│   ├── integration/
│   └── unit/
├── MANIFEST.in
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Principais componentes

### `main.py`

Arquivo de entrada da aplicação.

Responsabilidades:

- localizar a raiz do projeto procurando `config/settings.yaml`;
- ajustar o diretório de execução para a raiz do projeto;
- configurar logging;
- carregar o arquivo de configuração;
- criar a SparkSession;
- instanciar `DataHandler`, `Transformation` e `Pipeline`;
- executar o pipeline;
- finalizar a SparkSession com `spark.stop()`.

O arquivo gera um log na raiz do projeto com o nome:

```text
dataeng-pyspark-YYYYMMDD.log
```

### `config/settings.yaml`

Arquivo de configuração central do projeto.

Exemplo:

```yaml
spark:
  app_name: "Relatório de pedidos de venda"

paths:
  pagamentos: "data/input/dataset-json-pagamentos/data/pagamentos"
  pedidos: "data/input/datasets-csv-pedidos/data/pedidos/"
  output: "data/output/pedidos_por_cliente"

file_options:
  pedidos_csv:
    compression: "gzip"
    header: True
    sep: ";"
```

Esse arquivo define:

- nome da aplicação Spark;
- caminho de entrada dos pagamentos;
- caminho de entrada dos pedidos;
- caminho de saída do relatório;
- opções de leitura do CSV de pedidos.

### `DataHandler`

Classe responsável por entrada e saída de dados.

Responsabilidades:

- definir schema explícito para pedidos;
- definir schema explícito para pagamentos;
- ler pedidos em CSV compactado;
- ler pagamentos em JSON;
- gravar o resultado em Parquet com compressão Snappy.

O uso de schema explícito evita inferência automática de tipos e deixa a leitura mais previsível.

### `Transformation`

Classe responsável pelas regras de negócio.

Responsabilidades:

- normalizar a data do pedido;
- filtrar pedidos de 2025;
- padronizar UF;
- calcular valor total por pedido;
- filtrar pagamentos recusados e sem fraude;
- juntar pedidos e pagamentos;
- selecionar e ordenar as colunas finais.

Uma particularidade importante é o uso de `try_to_timestamp` em vez de `to_timestamp`. Essa escolha foi feita porque, no Spark 4, o parsing de data/timestamp é mais rígido. Com `try_to_timestamp`, formatos inválidos retornam `NULL` em vez de quebrar o job imediatamente.

A transformação aceita a data de origem em `data_criacao`, usada no dataset real, e também `data_pedido`, usada nos testes.

### `SparkSessionManager`

Classe responsável por criar a SparkSession local.

Configurações principais:

```python
.master("local[1]")
.config("spark.ui.enabled", "false")
.config("spark.sql.shuffle.partitions", "1")
.config("spark.default.parallelism", "1")
.config("spark.driver.host", "127.0.0.1")
.config("spark.driver.bindAddress", "127.0.0.1")
.config("spark.hadoop.fs.defaultFS", "file:///")
```

Essas configurações deixam a execução local mais estável e evitam que o Spark tente usar HDFS/YARN configurados no ambiente da máquina.

## Particularidades importantes do projeto

### 1. Execução local e independente de IDE

O projeto não depende de uma IDE específica. Pode ser executado por:

- PyCharm;
- VS Code;
- IntelliJ com plugin Python;
- terminal;
- ambiente local com `.venv`.

A forma mais segura é abrir a raiz do projeto na IDE:

```text
data-engineering-pyspark/
```

E não abrir apenas a pasta `src/`.

### 2. Uso de layout `src/`

O projeto usa o layout profissional com código dentro de `src/`:

```text
src/dataeng_pyspark_data_pipeline/
```

Por isso, a forma recomendada de execução é instalar o projeto em modo editável:

```bash
python -m pip install -e .
```

Depois disso, o comando `run-data-pipeline` fica disponível na `.venv`.

### 3. `SPARK_HOME` não é necessário

Como o PySpark é instalado via `pip`, o projeto não precisa de uma instalação global do Spark.

Se houver um `SPARK_HOME` apontando para outro Spark, podem ocorrer erros como:

```text
JavaPackage object is not callable
UnsupportedClassVersionError
travamento na criação da SparkSession
```

Se necessário, limpe a variável no terminal atual:

```bash
unset SPARK_HOME
```

### 4. Testes isolados do ambiente global

O arquivo `tests/conftest.py` remove variáveis que poderiam interferir nos testes:

```python
os.environ.pop("SPARK_HOME", None)
os.environ.pop("HADOOP_CONF_DIR", None)
os.environ.pop("YARN_CONF_DIR", None)
```

Isso evita que os testes herdem configurações externas de Spark, Hadoop, YARN ou HDFS da máquina do desenvolvedor.

### 5. Spark local com `local[1]`

O projeto usa `local[1]` para execução local e testes.

Essa escolha torna a execução mais previsível em notebooks, IDEs e estações de trabalho locais. Para volumes pequenos e ambiente de desenvolvimento, isso é suficiente.

## Como reproduzir o projeto ao baixar

### 1. Entrar na pasta do projeto

```bash
cd data-engineering-pyspark
```

### 2. Criar ambiente virtual

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Atualizar ferramentas básicas

```bash
python -m pip install --upgrade pip setuptools wheel
```

### 4. Instalar o projeto com dependências de desenvolvimento

Forma recomendada:

```bash
python -m pip install -e ".[dev]"
```

Essa instalação usa o `pyproject.toml`, instala o pacote em modo editável e também instala ferramentas de desenvolvimento como `pytest`, `pytest-cov`, `ruff`, `black` e `build`.

Forma alternativa usando `requirements.txt`:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

### 5. Conferir instalação

```bash
python --version
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

O PySpark esperado é:

```text
4.1.1
```

## Como executar o pipeline

### Forma recomendada: comando instalado pelo pacote

Após executar `python -m pip install -e .`, rode:

```bash
run-data-pipeline
```

Essa é a forma mais simples e mais portável.

### Forma alternativa: executar como módulo

Também é possível executar como módulo Python:

```bash
python -m dataeng_pyspark_data_pipeline.main
```

Essa forma exige que o projeto tenha sido instalado com:

```bash
python -m pip install -e .
```

Ou que `src` esteja no `PYTHONPATH`.

### Forma alternativa sem instalar o pacote

Linux/macOS:

```bash
PYTHONPATH=src python -m dataeng_pyspark_data_pipeline.main
```

Windows PowerShell:

```powershell
$env:PYTHONPATH="src"
python -m dataeng_pyspark_data_pipeline.main
```

### Forma pela IDE

Na IDE, use preferencialmente esta configuração:

```text
Project root: data-engineering-pyspark
Python interpreter: .venv/bin/python
Working directory: data-engineering-pyspark
Module name: dataeng_pyspark_data_pipeline.main
```

Caso a IDE não encontre os imports, execute antes:

```bash
python -m pip install -e .
```

Ou configure a pasta `src/` como source root.

## Como executar os testes

Sempre execute os testes a partir da raiz do projeto.

### Todos os testes

```bash
python -m pytest .
```

### Testes unitários

```bash
python -m pytest tests/unit
```

### Testes de integração

```bash
python -m pytest tests/integration
```

### Teste específico

```bash
python -m pytest tests/unit/test_transformations.py::TestTransformation::test_soma_multiplos_itens_do_mesmo_pedido -vv -s
```

### Testes com relatório de cobertura no terminal

```bash
python -m pytest . --cov=src --cov-report=term-missing
```

### Testes com relatório HTML de cobertura

```bash
python -m pytest . --cov=src --cov-report=html
```

Depois abra:

```text
htmlcov/index.html
```

### Observação sobre `pytest` direto

Prefira:

```bash
python -m pytest
```

Em vez de:

```bash
pytest
```

Isso garante que o `pytest` usado é o da `.venv` ativa.

## Como executar o linter

O projeto usa Ruff como linter.

### Verificar problemas

```bash
python -m ruff check src tests
```

### Corrigir automaticamente o que for seguro

```bash
python -m ruff check src tests --fix
```

## Como executar o formatador

O projeto usa Black como formatador.

### Verificar formatação sem alterar arquivos

```bash
python -m black --check src tests
```

### Formatar arquivos

```bash
python -m black src tests
```

## Como gerar build do projeto

Para gerar os artefatos de distribuição:

```bash
python -m build
```

Os arquivos serão gerados em:

```text
dist/
```

Normalmente serão criados:

```text
.tar.gz
.whl
```

## Como limpar arquivos gerados

Opcionalmente, para limpar arquivos locais gerados por execução, testes e build:

```bash
rm -rf data/output/pedidos_por_cliente
rm -rf dist build
rm -rf .pytest_cache .ruff_cache htmlcov .coverage
rm -rf src/*.egg-info *.egg-info
rm -f dataeng-pyspark-*.log
```

## Saída do pipeline

A saída é gravada em:

```text
data/output/pedidos_por_cliente
```

Formato:

```text
Parquet
```

Modo de escrita:

```text
overwrite
```

Compressão:

```text
snappy
```

## Logs

A cada execução, o projeto cria um arquivo de log na raiz:

```text
dataeng-pyspark-YYYYMMDD.log
```

Exemplo:

```text
dataeng-pyspark-20260628.log
```

O log registra:

- configuração do logging;
- início do pipeline;
- leitura de pedidos;
- leitura de pagamentos;
- aplicação das regras de negócio;
- escrita do Parquet;
- finalização da SparkSession;
- erros críticos, quando houver.

## Problemas comuns

### Erro: `.venv/bin/python: No such file or directory`

Significa que a `.venv` foi apagada ou corrompida, mas o terminal ainda mostra `(.venv)` no prompt.

Solução:

```bash
deactivate 2>/dev/null || true
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

### Erro relacionado a Java ou classe compilada

Exemplo:

```text
UnsupportedClassVersionError
```

Verifique se o Java é 17:

```bash
java -version
```

### Erro ou travamento com SparkSubmit antigo

Verifique processos Java/Spark:

```bash
jps -l -m
```

Se houver processos antigos de teste, como:

```text
org.apache.spark.deploy.SparkSubmit --conf spark.app.name=test-pipeline-session
```

Mate o processo antigo:

```bash
kill -9 PID
```

### Erro por mistura de Spark externo

Se existir `SPARK_HOME` apontando para outro Spark:

```bash
echo $SPARK_HOME
```

Limpe no terminal atual:

```bash
unset SPARK_HOME
```

Para ajuste permanente, remova ou comente `SPARK_HOME` do `~/.bashrc`, `~/.zshrc` ou configuração equivalente.

## Versionamento e arquivos que não devem ser enviados

Não é recomendado enviar para o repositório ou pacote final:

```text
.venv/
.idea/
.pytest_cache/
.ruff_cache/
__pycache__/
dist/
build/
*.egg-info/
data/output/
*.log
```

Os dados de entrada em `data/input/` podem ser mantidos no projeto se forem necessários para avaliação/reprodução local. Caso sejam grandes, documente a origem e deixe fora do repositório.

## Resumo rápido de comandos

```bash
cd data-engineering-pyspark

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"

run-data-pipeline

python -m pytest .
python -m pytest . --cov=src --cov-report=term-missing

python -m ruff check src tests
python -m ruff check src tests --fix

python -m black --check src tests
python -m black src tests

python -m build
```

## Autores

- Fernanda Vitoria de Oliveira Souza
- Gabriell Cavalcante
- Gabriel Pereira Viana
- Thiago Buttler