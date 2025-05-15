import os
import pandas as pd
from datetime import datetime
import locale
from dateutil.relativedelta import relativedelta
import csv

from api_consultas.funtions_main import *
from cache_manager import *

locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')


# === FUNÇÕES DE CARGA E PRÉ-PROCESSAMENTO === #

def carregar_dados_cache():
    """Carrega e armazena em cache os dados necessários"""
    CACHE(rank_arena_all_data(), 'todos_dados_api_rank_arena').save_dataframe_to_cache()
    CACHE(pd.read_excel(r"C:\Users\agata\CBW 2025\BOLSA ATLETA\PREVISAO_PRESTACAO_DE_CONTAS_BOLSA_ATLETA.xlsx"),
          'dados_prestacao_2025').save_dataframe_to_cache()


def combinar_infos_por_nome(df_dados: pd.DataFrame, coluna_nome: str, tipo_merge: str = 'left') -> pd.DataFrame:
    """Combina um DataFrame com dados do SGE a partir do nome completo"""
    df_sge = CACHE(cache_file_name='cpf_id_nome_atleta').load_dataframe_from_cache()
    return df_dados.merge(df_sge, how=tipo_merge, left_on=coluna_nome, right_on='nome_completo')


def filtrar_resultados_ultimo_ano(df_resultados: pd.DataFrame, id_atleta: int, data_referencia: datetime) -> pd.DataFrame:
    """Filtra os resultados do atleta nos últimos 12 meses"""
    data_referencia = parse_date(data_referencia)
    data_inicio = data_referencia - relativedelta(years=1)

    df_resultados = df_resultados[
        (~df_resultados['customId'].isna()) &
        (df_resultados['customId'] != 'None')
    ]

    df_resultados['data_fim'] = df_resultados['data_fim'].apply(parse_date)
    df_resultados['customId'] = df_resultados['customId'].astype('Int64')

    return df_resultados[
        (df_resultados['data_fim'] >= data_inicio) &
        (df_resultados['customId'] == id_atleta)
    ]


# === FUNÇÕES DE FORMATAÇÃO === #

def gerar_lista_resultados_html(df_resultados: pd.DataFrame, limit: int) -> str:
    """Gera a lista HTML formatada com os resultados do atleta"""

    if df_resultados.empty:

        return '<ul style="font-family: Calibri, sans-serif; font-size: 10pt;">Atleta sem resultados durante o pleito</ul>'

    lista = []

    for n, (_, row) in enumerate(df_resultados.iterrows()):

        if n >= limit:
            break

        rank = row['rank']
        prova = row['name']
        estilo = map_style(row['sportAlternateName'])
        classe = row['audienceName']
        evento = row['descricao']
        data = full_date(row['data_fim'])
        local = local_formatado(row['local'])

        texto = f'<li>{rank}ª classificação na prova {prova}, no estilo {estilo}, na classe {classe}, no {evento}, realizado no dia {data}, na cidade de {local}.</li>'
        lista.append(texto.replace("Ã§", "ç"))

    return f'<p><ul style="font-family: Calibri, sans-serif; font-size: 10pt;">{"".join(lista)}</ul></p>'


# === FUNÇÕES DE PROCESSAMENTO PRINCIPAL === #

def gerar_dataframe_resultados(df_ministerio: pd.DataFrame) -> pd.DataFrame:
    """Gera o DataFrame final com as informações e resultados formatados"""

    resultados_api = CACHE(cache_file_name='todos_dados_api_rank_arena').load_dataframe_from_cache()
    eventos_info = CACHE(cache_file_name='dados_eventos_2023a2025').load_dataframe_from_cache()
    resultados_api = resultados_api.merge(eventos_info, how='left', left_on='id_evento', right_on='id')

    df_ministerio['id_atleta'] = df_ministerio['id_atleta'].astype('Int64')
    df_ministerio['cpf'] = df_ministerio['cpf'].apply(format_cpf)

    registros = []

    for _, row in df_ministerio.iterrows():

        id_atleta = row['id_atleta']
        categoria = row['categoria_bolsa']
        nome = row['nome_completo']
        cpf = row['cpf']
        data_fim = row['data_final']

        resultados_filtrados = filtrar_resultados_ultimo_ano(resultados_api, id_atleta, data_fim)
        resultados_formatados = gerar_lista_resultados_html(resultados_filtrados, 6)

        registros.append({
            'id': id_atleta,
            'tipo': 'atleta',
            'escopo': categoria,
            'nome_completo': nome,
            'cpf': cpf,
            'resultados_compilados': resultados_formatados
        })

    return pd.DataFrame(registros)


def salvar_csv(df: pd.DataFrame, nome_arquivo: str):
    """Salva o DataFrame como CSV formatado para o Ministério"""
    os.makedirs(os.path.dirname(nome_arquivo), exist_ok=True)
    df.to_csv(nome_arquivo, index=False, sep=";", quoting=csv.QUOTE_MINIMAL)


# === MAIN === #

def main():
    """Executa o processo completo de geração dos resultados da prestação"""
    # carregar_dados_cache()

    current_month = datetime.now().month
    df_ministerio = CACHE(cache_file_name='dados_prestacao_2025').load_dataframe_from_cache()

    # Filtra só os atletas com prestação no mês atual
    df_ministerio = df_ministerio[df_ministerio['data_final'].dt.month == current_month]

    # Adiciona infos de id e cpf pelo nome
    df_completo = combinar_infos_por_nome(df_ministerio, 'nome_completo')

    # Gera resultados e salva
    df_resultados = gerar_dataframe_resultados(df_completo)

    caminho_csv = fr"C:\Users\agata\CBW 2025\BOLSA ATLETA\CSV_PRESTAÇÃO_MÊS_{current_month}.csv"

    salvar_csv(df_resultados, caminho_csv)


if __name__ == '__main__':

    main()
