import pandas as pd

from api_consultas.funtions_main import *
from cache_manager import *
from utils.save_files import save_df
from utils.maps_formats import map_subcategoria_etaria, map_style_extense, parse_date_normal, map_style_sge, map_genero
import numpy as np


def consultar_cpf(id_atleta):

    token = "a2d0Q4upQfZas0wyXIbPRHfCembs9HbN"
    expands = ["atletaDocumentos, estabelecimento"]
    base_url = f"https://restcbw.bigmidia.com/gestao/api/atleta/{id_atleta}"
    headers = {"Content-Type": "application/json"}
    querys = f"?expand={','.join(map(str, expands))}&access-token={token}"

    response = requests.get(f"{base_url}{querys}", headers=headers).json()

    cpf = None
    uf = response['estabelecimento']['uf']

    for documento in response['atletaDocumentos']:
        if documento['id_tipo'] == 1:
            cpf = documento['numero']

            break

    return cpf, uf


def listar_code_alt_atletas_internacionais():

    credentials = {"api_key": "jGpEm51R2k5nYZ6uVtcqjBnBsksrcUhteFfgFXPLhysSiW37he",
                   "client_id": "b020a718926bba9cb4053adcb4cd22fc",
                   "client_secret": "89c383a23f21a3f0f33f8ddbe6194b89ec13cbe7e401416875cfdbea3740b3173178250509f28fe22bff23040232e2dd59c7041a907084aa1ecb15f0ddbb7153",
                   "ip": "localhost",
                   "event_id": "1f0a3f5e-bdab-6e26-8ba8-f9a431d14600",
                   "directory": "none",
                   "user_name": "sula25"}

    def get_token(api_key, client_id, client_secret, ip):
        url = f'http://{ip}:8080/oauth/v2/token'
        params = {
            'grant_type': 'https://arena.uww.io/grants/api_key',
            'client_id': client_id,
            'client_secret': client_secret,
            'api_key': api_key
        }
        response = requests.post(url, params=params)
        return response.json()["access_token"]

    headers = {'Authorization': 'Bearer ' + get_token(credentials['api_key'], credentials['client_id'], credentials['client_secret'], credentials['ip'])}

    def get_endpoint_response(headers, endpoint):
        url = f"http://localhost:8080/api/json/{endpoint}"
        response = requests.get(url, headers=headers)
        return response.json()

    atletas_json = get_endpoint_response(headers, 'athlete/1f0a3f5e-bdab-6e26-8ba8-f9a431d14600?limit=1000')

    nome_code_dict = {}

    for item in atletas_json['athletes']['items']:
        nome_code_dict[item['personFullName']] = item['teamAlternateName']

    CACHE(cache_instance=nome_code_dict, cache_file_name='atleta_internacional_pais_dict').save_dataframe_to_cache()

    return nome_code_dict


def consulta_pais(nome, nome_code_dict):

    pais = None

    for chave, valor in nome_code_dict.items():

        if chave == nome:
            pais = valor
            break

    return pais


def process_row(row, nome_code_dict=None):

    id_atleta_do_individuo = row['customId']
    nome_individuo = row['fullName']

    if not nome_code_dict:
        cpf, uf = consultar_cpf(id_atleta_do_individuo)
        row['Cpf'] = cpf
        row['Uf'] = uf

    else:
        row['Cpf'] = 'estrangeiro'
        row['Uf'] = consulta_pais(nome_individuo, nome_code_dict)
        row['Uf2'] = consulta_pais(nome_individuo, nome_code_dict)

    return row


def run_main_results():

    ids = {
        "nacional": [
            '177',  # BRA U23
            '160',  # BRA U17
            '153',  # BRA U15
            '179',  # BRA Sênior
        ],
        "pan": [
            '156',  # PAN U20
            '155',  # PAN U17
            '150',  # PAN SÊNIOR
        ],
        "sula": [
            '175',  # SUL-AMERICANO
        ],
    }

    def to_int(series):
        return series.astype(str)

    # Carregar DFS
    data = CACHE(cache_file_name='all_rank_arena_data').load_dataframe_from_cache()
    cpf_data = CACHE(cache_file_name='cpf_id_nome_atleta').load_dataframe_from_cache()
    evento_data = CACHE(cache_file_name='dados_eventos_2025').load_dataframe_from_cache()
    nome_code_dict = CACHE(cache_file_name='atleta_internacional_pais_dict').load_dataframe_from_cache()
    ranking_nacional_25 = CACHE(cache_file_name='ranking_nacional_2025').load_dataframe_from_cache()

    def format_ranking(df):

        df = df[df['categoria'].isin(['Sênior', 'Sub-20', 'Sub-17'])]

        df['descricao'] = ('Ranking Nacional ' + df['categoria'].astype(str))

        df['peso_norm'] = df['peso'].astype(str).str.replace(r'kg', ' kg', regex=True)

        df.rename(columns={'id_atleta': 'customId',
                           "colocacao": 'rank',
                           'nome_completo': 'atleta.nome_completo',
                           'estilo': 'sportName',
                           'categoria': 'audienceName',
                           'peso_norm': 'name',
                           'federacao_uf': 'atleta.estabelecimento.uf'}, inplace=True)

        df['data_inicio'] = '31/12/2025'
        df['local'] = 'Ranking'
        df['atleta.sexo'] = df['sportName'].apply(map_genero)
        df['sportAlternateName'] = df['sportName'].apply(map_style_sge)
        df['customId'] = df['customId'].astype(str)
        df['code'] = 'BRA'

        return df

    df_ranking = format_ranking(ranking_nacional_25)

    data['code'] = 'BRA'
    data['code'] = data['fullName'].map(nome_code_dict).fillna('BRA')

    # Padronizar colunas de ID
    cols_data = ["customId", "id_evento"]
    cols_cpf = ["id_atleta"]
    cols_evento = ["id"]

    for col in cols_data:
        data[col] = to_int(data[col])

    for col in cols_cpf:
        cpf_data[col] = to_int(cpf_data[col])

    for col in cols_evento:
        evento_data[col] = to_int(evento_data[col])

    data_cpf = data.merge(cpf_data, how='left', left_on='customId', right_on='id_atleta')
    df_ranking = df_ranking.merge(cpf_data, how='left', left_on='customId', right_on='id_atleta')

    data_cpf_evento = data_cpf.merge(evento_data, how='left', left_on='id_evento', right_on='id')

    all_ids = ids["nacional"] + ids["pan"] + ids["sula"]

    data_filter = data_cpf_evento[data_cpf_evento['id_evento'].isin(all_ids)]

    def format_events_df(df):

        df['subcategoria_etaria'] = df['audienceName'].apply(
            map_subcategoria_etaria)

        df['modalidade'] = df['sportName'].apply(map_style_extense)

        df['classe_funcional'] = 'Não se aplica'

        pesos_olimpicos = {'FS': ['57 kg', '65 kg', '74 kg', '86 kg', '97 kg', '125 kg'],
                           'WW': ['50 kg', '53 kg', '57 kg', '62 kg', '68 kg', '76 kg'],
                           'GR': ['60 kg', '67 kg', '77 kg', '87 kg', '97 kg', '130 kg']}

        df['peso_olimpico'] = df.apply(
            lambda row: row['name'] in pesos_olimpicos.get(row['sportAlternateName'], []),
            axis=1
        )

        df['categoria_olimpica'] = (
                (df['subcategoria_etaria'].isin(['Principal', 'Intermediária'])) &
                (df['peso_olimpico'])
        ).map({True: 'Sim', False: 'Não'})

        df['uf_ou_code'] = df.apply(
            lambda row: row['code'] if row['escopo'] == 'Internacional'
            else row['atleta.estabelecimento.uf'],
            axis=1
        )

        df['nome_final'] = df.apply(
            lambda row: row['fullName'].upper() if row['id_evento'] == '175'
            else row['atleta.nome_completo'],
            axis=1
        )

        df['uf_final'] = df.apply(
            lambda row: row['code'] if row['code'] != 'BRA'
            else row['atleta.estabelecimento.uf'],
            axis=1
        )

        df['distinct_count_uf'] = (df.groupby(['id_classe_peso', 'descricao', 'audienceName'])['uf_ou_code']
                                   .transform('nunique'))
        df['name_count_uf'] = (df.groupby(['id_classe_peso', 'descricao', 'audienceName'])['uf_ou_code']
                               .transform(lambda x: ', '.join(x.unique())))

        df = df[
            np.where(
                df['id_evento'].isin(['156', '155', '150']),
                df['rank'] <= 3,  # se o evento está na lista → até 3º
                df['rank'] <= 6  # caso contrário → até 6º
            )
        ]

        df['data_inicio'] = df['data_inicio'].apply(parse_date_normal)

        colunas_map = {
            "EVENTO": 'descricao',
            "MODALIDADE": 'modalidade',
            "Data do evento": 'data_inicio',
            "Local do evento": 'local',
            "Representatividade do evento": 'escopo',
            "Subcategoria etária": 'subcategoria_etaria',
            "Prova": 'name',
            "PROVA OLÍMPICA": 'categoria_olimpica',
            "Gênero": 'atleta.sexo',
            "Classificação": 'rank',
            "Nome do atleta": 'nome_final',
            "CPF do atleta": 'cpf',
            'UF atleta': 'uf_final',
            "CLASSE FUNCIONAL": 'classe_funcional',
            "QUANT. DE UF NO EVENTO (numero)": 'distinct_count_uf',
            "QUANT. DE ESTADOS NA PROVA (nomes dos UF)": 'name_count_uf'
        }

        colunas_existentes = {k: v for k, v in colunas_map.items() if v in df.columns and v != ""}

        # Seleciona as colunas
        df_final = df[list(colunas_existentes.values())].copy()

        # Renomeia para os nomes desejados (as chaves)
        df_final = df_final.rename(columns={v: k for k, v in colunas_existentes.items()})

        return df_final

    concat = pd.concat([data_filter, df_ranking], ignore_index=True)

    final = format_events_df(concat)

    save_df(final, "xlsx")


if __name__ == '__main__':

    run_main_results()





