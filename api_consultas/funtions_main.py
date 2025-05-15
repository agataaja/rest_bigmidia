import datetime
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

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


def main_ranking(id_classe_peso):

    url_rank_category = f"https://www.cbw.org.br/api/evento-atleta/rank-atual?sort=colocacao&ano=2024&grupo=NACIONAL&id_classe_peso="
    querys = f'{id_classe_peso}'
    headers = {"Content-Type": "application/json"}
    page_count = requests.get(f"{url_rank_category}{querys}").json()["_meta"]["page_count"]
    # response_ranking = requests.get(url_rank_category).json()['items']

    with ThreadPoolExecutor() as executor:
        dfs = executor.map(lambda page: fetch_data(url_rank_category, querys, headers, page),
                            range(1, page_count + 1))
    try:
        final = pd.concat(dfs, ignore_index=True)
    except:
        final = pd.DataFrame(dfs)

    return final


def return_all_ranking_2024():

    all_Df = all_id_classe_peso()

    dfs_list = []

    for index, row in all_Df.iterrows():

        df_ranking = main_ranking(row['id_classe_peso'])

        dfs_list.append(df_ranking)

    all_ranking = pd.concat(dfs_list)

    return all_ranking


def all_id_classe_peso():

    url_lista_peso = f"https://www.cbw.org.br/api/evento-atleta/rank-lista-pesos"

    response_pesos = requests.get(url_lista_peso).json()['items']['2024']['GERAL']

    dfs = []

    estilo_map = {'Estilo Livre - Fem.': 'WW', 'Estilo Livre - Masc.': 'FS', 'Greco-Romano - Masc.': 'GR'}

    idade_map = {'Infantil 11 e 12': 'inf 11-12', 'Sênior': 'seniors', 'Sub-15': 'u15', 'Sub-17': 'u17',
                 'Sub-20': 'u20',
                 'Veteranos A': 'veterans-a'}

    for estilo, age_groups in response_pesos.items():

        for age_group, weights in age_groups.items():
            df = pd.json_normalize(weights)

            df['estilo'] = estilo

            df['age_group'] = age_group

            df['style'] = df['estilo'].map(estilo_map)

            df['age_group'] = df['age_group'].map(idade_map)

            dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)

    return result_df


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
        lambda x: datetime.datetime.fromtimestamp(x).strftime('%d/%m/%Y')
        if pd.notna(x) and isinstance(x, (int, float)) else 'Invalid Timestamp'
    )
    print(final_df['created_at'])
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
    expands = [ ]# ["classe,modalidade,classePeso,atleta,atletaGestao,estabelecimento"]
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={expands}"

    page_count = requests.get(f"{base_url}{querys}", headers=headers).json()["_meta"]["pageCount"]

    with ThreadPoolExecutor() as executor:

        dfs = executor.map(lambda page: fetch_data(base_url, querys, headers, page), range(1, page_count+1))

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def percentuais_regiao_e_estado ():

    df_estabelecimento = main_estabelecimento() # Comment this line if not needed

    df_atletas = main_atletas()  # Assign the DataFrame to a variable

    region_mapping = {
        'AC': 'Norte',
        'AL': 'Nordeste',
        'AP': 'Norte',
        'AM': 'Norte',
        'BA': 'Nordeste',
        'CE': 'Nordeste',
        'DF': 'Centro-Oeste',
        'ES': 'Sudeste',
        'GO': 'Centro-Oeste',
        'MA': 'Nordeste',
        'MT': 'Centro-Oeste',
        'MS': 'Centro-Oeste',
        'MG': 'Sudeste',
        'PA': 'Norte',
        'PB': 'Nordeste',
        'PR': 'Sul',
        'PE': 'Nordeste',
        'PI': 'Nordeste',
        'RJ': 'Sudeste',
        'RN': 'Nordeste',
        'RS': 'Sul',
        'RO': 'Norte',
        'RR': 'Norte',
        'SC': 'Sul',
        'SP': 'Sudeste',
        'SE': 'Nordeste',
        'TO': 'Norte'
    }

    df_atletas['região'] = df_atletas['estabelecimento.uf'].map(region_mapping)

    contagem_regiao = df_atletas.groupby(['região']).size()
    contagem_total_regiao = contagem_regiao.sum()
    percentual_regiao = contagem_regiao/contagem_total_regiao*100

    contagem_estado = df_atletas.groupby(['estabelecimento.uf']).size()
    contagem_total_estado = contagem_estado.sum()
    percentual_estado = contagem_estado/contagem_total_estado*100

    print("percentual regiao:",percentual_regiao, "percentual estado:",percentual_estado)

    concat_list = []

    for eventos in get_ids_ano_eventos([2023])['id'].tolist():

        try:
            df = rank_arena_atleta(eventos)
            concat_list.append(df)

        except(ValueError):

            print("id:", eventos, " sem atletas")

    df_global = pd.concat(concat_list, ignore_index=True)

    #df_global.to_excel("compiled_2023_sge_data.xlsx", sheet_name="Compiled")
def format_cpf(var):

    cpf = str(var).zfill(11)  # Garante que tenha 11 dígitos

    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"

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


def map_by_region(df, uf_col_name):

    region_mapping = {
        'AC': 'Norte',
        'AL': 'Nordeste',
        'AP': 'Norte',
        'AM': 'Norte',
        'BA': 'Nordeste',
        'CE': 'Nordeste',
        'DF': 'Centro-Oeste',
        'ES': 'Sudeste',
        'GO': 'Centro-Oeste',
        'MA': 'Nordeste',
        'MT': 'Centro-Oeste',
        'MS': 'Centro-Oeste',
        'MG': 'Sudeste',
        'PA': 'Norte',
        'PB': 'Nordeste',
        'PR': 'Sul',
        'PE': 'Nordeste',
        'PI': 'Nordeste',
        'RJ': 'Sudeste',
        'RN': 'Nordeste',
        'RS': 'Sul',
        'RO': 'Norte',
        'RR': 'Norte',
        'SC': 'Sul',
        'SP': 'Sudeste',
        'SE': 'Nordeste',
        'TO': 'Norte'
    }

    df['region'] = df[uf_col_name].map(region_mapping)
    return df


def percentuais_anuais_inscritos ():

    ids_list_2024 = [77, 78, 79]

    _ids_list_2023 = [39, 38, 60]

    ids_ano_list = get_ids_ano_eventos([2023])['id'].tolist()

    print(ids_ano_list)

    df_estabelecimento = main_estabelecimento()

    # print(inscritos_eventos_por_id(82).columns)

    map_df = []

    for i in ids_list_2024:

        map_df.append(inscritos_eventos_por_id(i))

    df_inscritos = pd.concat(map_df, ignore_index=True)

    #df_inscritos_2023 = pd.concat((list(map(inscritos_eventos_por_id, ids_ano_list))), ignore_index=True)

    merged_df = pd.merge(df_inscritos,
                         df_estabelecimento,
                         "left",
                         left_on="id_estabelecimento",
                         right_on="id")

    print(merged_df.columns)

    print(map_by_region(merged_df, "uf").groupby("uf").size())

    print(len(df_inscritos)/len(df_inscritos))
    print(len(df_inscritos), len(df_inscritos))

    print(len(inscritos_eventos_por_id(82)), len(inscritos_eventos_por_id(36)))
    print(len(inscritos_eventos_por_id(82))/len(inscritos_eventos_por_id(36)))


    inscricoes_por_estado = map_by_region(merged_df, "uf").groupby("Region").size()

    df = inscricoes_por_estado.reset_index()

    df.columns = ['Região', 'inscricoes']

    total_inscricoes = df['inscricoes'].sum()

    # Calcular a participação de cada estado no total de inscrições
    df['participacao'] = df['inscricoes'] / total_inscricoes

    # Calcular o quadrado da participação de cada estado
    df['participacao_quadrado'] = df['participacao'] ** 2

    # Calcular o IHH (somando os quadrados da participação de cada estado)
    ihh = df['participacao_quadrado'].sum()

    print("IHH:", ihh)


def lista_por_equipes():

    df = main_atletas()


    # evento cubatão
    # evento cubatão
    filtered_df_semifinais_cubatao = df[df['estabelecimento.id'].isin([79, 84, 81])]
    lista_ids_atletas = filtered_df_semifinais_cubatao['id'].tolist()
    print("evento cubatão:", lista_ids_atletas)

    # evento niteroi
    # evento niteroi
    filtered_df_semifinais_niteroi = df[df['estabelecimento.id'].isin([69, 108])]
    lista_ids_atletas = filtered_df_semifinais_niteroi['id'].tolist()
    print("evento niteroi:", lista_ids_atletas)


def atletas_exercito():

    id_gatinhas = [110, 203, 246, 167, 98, 129, 621, 41, 110, 104, 106, 228, 291]

    df = main_atletas()

    filtered_gatinhas = df[df['id'].isin(id_gatinhas)]

    f =rank_arena_atleta_resultados([2023, 2024], filtered_gatinhas)

    f.to_excel('exercito.xlsx')

def map_style(var):
    if 'GR' in var:
        return "Greco-Romano"

    elif var == 'FS':
        return 'Livre Masculino'

    else:
        return 'Livre Feminino'

def full_date(var):
    try:

        return datetime.strptime(str(var), "%Y-%m-%d %H:%M:%S").strftime(
            "%d de %B de %Y")
    except:
        return var

def local_formatado(var):

    return var.encode('utf-8').decode('utf-8')

def map_style_extense(var):

    if 'Greco' in var:
        return "Greco-Romano"

    elif 'Female' in var:
        return 'Livre Feminio'

    else:
        return 'Livre Masculino'


def parse_date(value):
    """
    Converte qualquer entrada de data para datetime.datetime,
    ou retorna pd.NaT se não for possível converter.
    """
    if pd.isna(value):
        return pd.NaT

    if isinstance(value, datetime):
        return value

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, str):
        # Tenta múltiplos formatos comuns
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue

    return pd.NaT  # Se nada funcionar


def to_inteiro(var):
    try:
        return int(var)
    except Exception as e:
        print('exceção:', e)
        return var