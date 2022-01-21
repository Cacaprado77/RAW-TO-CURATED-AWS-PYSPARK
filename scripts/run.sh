#!/usr/bin/python3.8

# command pra testar localmente
# alterar o nome do arquivo main, caso queira rodar o main_estruturada
COMMAND="/opt/spark1/bin/spark-submit \
    --deploy-mode client \
    --name 'Raw to Curated AWS' \
    --py-files /home/caca/vscode/ETL_integracoes/raw_curated_aws_v2/utils.py /home/caca/vscode/ETL_integracoes/raw_curated_aws_v2/main_classe.py \
    "/home/caca/vscode/ETL_integracoes/raw_curated_aws_v2/raw/despachantes.csv" \
    "/home/caca/vscode/ETL_integracoes/raw_curated_aws_v2/curated/" "

echo "$COMMAND"
# após rodar o comando acima, copiar o output e colar no terminal para executar o job

# command pra rodar em cluster AWS
COMMAND="spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --name 'Raw to Curated AWS' \
    --py-files s3://dlake-emr-code-caca/projects/raw-to-curated/utils.py s3://dlake-emr-code-caca/projects/raw-to-curated/raw_curated_aws_classe.py s3://dlake-emr-code-caca/projects/raw-to-curated/main_classe.py \
    "s3://raw-zone-caca/despachantes.csv" \
    "s3://curated-zone-caca/parquet-files/" "

# verificar antes o IP SSH EMR para trocar abaixo e conectar no EMR
ssh -i ~/caca-keypairs.pem hadoop@ec2-3-226-76-133.compute-1.amazonaws.com $COMMAND

