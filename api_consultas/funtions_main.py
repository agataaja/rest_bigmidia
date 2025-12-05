import datetime
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from utils.maps_formats import parse_date


def fetch_data(base_url, querys, headers, page):

    response = requests.get(f"{base_url}{querys}&page={page}", headers=headers).json()['items']
    return pd.json_normalize(response)


def fetch_data_cpf(base_url, querys, headers, page):

    response = requests.get(f"{base_url}{querys}&page={page}", headers=headers).json()['items']
    dfs = []

    for item in response:

        cpf = None
        id_atleta = item['id']
        nome_completo = item['nome_completo'].upper()

        for documento in item['atletaDocumentos']:

            if documento['id_tipo'] == 1:

                cpf = documento['numero']

                break

        df = pd.DataFrame({
        "id_atleta": [id_atleta],
        "cpf": [cpf],
        "nome_completo": [nome_completo]})

        dfs.append(df)

    final = pd.concat(dfs)

    return final


def main_atletas():

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = ["atletaDocumentos, estabelecimento"]
    base_url = "https://restcbw.bigmidia.com/gestao/api/atleta"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&flag_del=0&access-token={token}"
    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    final_df['data_create'] = final_df['data_create'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%d-%m-%Y'))

    return final_df


def main_pessoas():

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = ["atletaDocumentos, estabelecimento"]
    base_url = "https://restcbw.bigmidia.com/gestao/api/pessoa"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&flag_del=0&access-token={token}"
    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    # print(final_df)
    return final_df


def rank_arena_atleta_resultados(ano, df):

    concat_list = []

    events_data = get_ids_ano_eventos(ano)

    for eventos in events_data['id'].tolist():

        try:

            df100 = rank_arena_atleta(eventos)
            merge = pd.merge(df100,
                             events_data,
                             how='left',
                             left_on='id_evento',
                             right_on='id')
            concat_list.append(merge)

        except(ValueError):

            pass

    df_global = pd.concat(concat_list, ignore_index=True)

    individual_results_list = []

    for id in df["id"]:
        filtered_df = df_global[df_global['customId'] == str(id)]

        individual_results_list.append(filtered_df)

    individual_results_df = pd.concat(individual_results_list, ignore_index=True)

    return individual_results_df


def main_estabelecimento():

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = [""]
    base_url = " https://restcbw.bigmidia.com/gestao/api/estabelecimento"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&flag_del=0&access-token={token}"
    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:
        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    filtered_df = final_df[~final_df['id_estabelecimento_tipo'].isin([1, 2])]

    final_df['created_at'] = final_df['created_at'].apply(
        parse_date
    )

    return filtered_df


def get_ids_ano_eventos(anos):

    headers = {"Content-Type": "application/json"}
    base_url = "https://restcbw.bigmidia.com/gestao/api/evento"
    querys = f"?flag_del=0"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    dfs = []

    with ThreadPoolExecutor() as executor:
        for ano in anos:
            filtered_dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))
            final_df = pd.concat(filtered_dfs, ignore_index=True)
            filtered_df = final_df[final_df['data_fim'].str.contains(f"{ano}")]
            dfs.append(filtered_df)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def consultar_cpf_all():

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = ["atletaDocumentos, estabelecimento"]
    base_url = f"https://restcbw.bigmidia.com/gestao/api/atleta"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&access-token={token}"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data_cpf(base_url, querys, headers, page), range(1, page_count + 1))

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def consultar_cpf(id_atleta):

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = ["atletaDocumentos, estabelecimento"]
    base_url = f"https://restcbw.bigmidia.com/gestao/api/atleta/{id_atleta}"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&access-token={token}"
    print(f"{base_url}{querys}")
    response = requests.get(f"{base_url}{querys}", headers=headers).json()
    print(response)
    cpf = None

    for documento in response['atletaDocumentos']:

        if documento['id_tipo'] == 1:

            cpf = documento['numero']

            break
    return cpf


def inscritos_eventos_por_id(id_evento):

    base_url = "https://restcbw.bigmidia.com/cbw/api/evento-atleta"
    expands = ["classe,modalidade,classePeso,atleta,atletaGestao,estabelecimento"]
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={expands}&id_evento={str(id_evento)}"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def rank_arena_atleta(id_evento):

    base_url = "https://restcbw.bigmidia.com/cbw/api/resultado-rank-arena"
    expands = ["classe,modalidade,classePeso,atleta,atletaGestao,estabelecimento"]
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={expands}&id_evento={str(id_evento)}"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def rank_arena_all_data():

    base_url = "https://restcbw.bigmidia.com/cbw/api/resultado-rank-arena"
    expands = []  # ["classe,modalidade,classePeso,atleta,atletaGestao,estabelecimento"]
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={expands}"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def estabelecimentos_logos():

    df = main_estabelecimento()
    print(df)
    for index, row in df.iterrows():

        id = str(row['id'])
        url = row['urlLogo']
        transformed_url = url.replace("cbw.bigmidia.com", "sge.cbw.org.br")

        response = requests.get(transformed_url)

        with open(f"{id}.png", 'wb') as f:
            f.write(response.content)


