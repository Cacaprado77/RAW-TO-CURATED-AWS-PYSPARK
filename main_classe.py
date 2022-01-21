from raw_curated_aws_classe import RawToCuratedAWS

import sys


def main():
    source_path = sys.argv[1]
    target_path = sys.argv[2]

    job = RawToCuratedAWS()

    df = job.leitura_csv(source_path)

    print("\nSPARK SESSION INICIALIZADO\n")
    df.show()

    print(f"\nARQUIVO LIDO DA ZONA RAW: {source_path}\n")

    df_split = job.quebra_string_tipo(df)
    df_split.show()

    print("\nDATAFRAME TRANSFORMADO\n")

    df_media_vendas = job.calcula_media_vendas(df_split)
    df_media_vendas.show()

    print("\nDATAFRAME CALCULADO\n")

    df_agrupado = job.agrupa_cidade_vendas(df)
    df_agrupado.show()

    print("\nDATAFRAME AGRUPADO\n")

    job.escreve_arquivo_parquet(df_agrupado, target_path)

    print(f"\nARQUIVO SALVO NA ZONA CURATED: {target_path}\n")
    print("\n>>>>>>> CÓDIGO CONCLUÍDO COM SUCESSO <<<<<<<<\n")

if __name__ == "__main__":
    main()