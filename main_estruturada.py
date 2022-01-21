import sys

from raw_curated_aws_estruturada import agrupa_cidade_vendas, \
                                        calcula_media_vendas, \
                                        escreve_arquivo_parquet, \
                                        leitura_csv, quebra_string_tipo


def main():
    source_path = sys.argv[1]
    target_path = sys.argv[2]

    df = leitura_csv(source_path)
    df.show()
    print("\nLeitura realizada\n")

    df_split = quebra_string_tipo(df)
    df_split.show()
    print("\nTransformação realizada\n")

    df_media_vendas = calcula_media_vendas(df_split)
    df_media_vendas.show()
    print("\nCálculo realizado\n")

    df_agrupado = agrupa_cidade_vendas(df_split)
    df_agrupado.show()
    print("\nAgrupamento realizado\n")

    escreve_arquivo_parquet(df_split, target_path)
    print("\nArquivo escrito na Zona Curated\n")
    print("\n>>>>>>> CÓDIGO CONCLUÍDO COM SUCESSO <<<<<<<<\n")

if __name__ == "__main__":
    main()

