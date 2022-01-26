# Raw-To-Curated- AWS Cloud - Pyspark Job

![](https://github.com/san089/Udacity-Data-Engineering-Projects/blob/master/image.jpeg)

Neste repositório você vai encontrar a etapa de movimentação de um determinado arquivo .CSV da Zona Raw, realizando algumas transformações e agregações, e escrevendo no formato .PARQUET na Zona Curated, pela **AWS CLOUD** através do OS Linux.

Foram feitas 2 propostas de Job, uma no modelo de `código estruturado` e outra `orientada a objetos` passando parâmetros no spark-submit.

Foi desenvolvido, conforme descrição abaixo.

1. Leitura dos dados de origem na Zona Raw no formato .CSV;
2. Transformações e agregações dos dados;
3. Escrita dos dados no destino na Zona Curated no formato .PARQUET particionado.

# Fontes

- `Leitura (origem):` [s3://raw-zone-caca/despachantes.csv]
- `Escrita (destino):` [s3://curated-zone-caca/parquet_files/]

# Transformações

1. Correção de nome
2. Renomeação de coluna
3. Exclusão de coluna
4. Separação de coluna com nome e sobrenome
5. Cálculo da média de vendas de uma determinada cidade
6. Agrupamento das cidades com suas respectivas vendas totais

# Execução

O arquivo [run.sh](scripts/run.sh) contém os comandos necessários para a execução do código.

---------------------------------------------------------------------------------------------------------------------------------------------------------

# Caso queira criar um amviente VENV dentro do Vscode com PIPENV

Primeiramente entramos no diretório do projeto e seguimos os seguintes passos:

1. pip3 install pipenv **caso não tenha instalado o pipenv**
2. set PIPENV_VENV_IN_PROJECT
3. PIPENV_VENV_IN_PROJECT=1 pipenv shell
4. pipenv install pyspark **instalar todas as lib necessárias para o projeto**
5. Criar a pasta Makefile na raiz do projeto com o seguinte script:
build:
	mkdir ./packages
	cd ./src && zip -r ../packages/src.zip .

---------------------------------------------------------------------------------------------------------------------------------------------------------
