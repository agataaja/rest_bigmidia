from api_consultas.funtions_main import *
from cache_manager import *
from utils.save_files import save_df


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

    credentials = {
        "api_key": "umtGezJZXZEj8cRmvPQzZEU8SNkRuN7mFfmuLypa6rpJUKvZ5A",
        "client_id": "d1532eae14baf3ff798b6c7be8a8355f",
        "client_secret": "2356194b47bbcb3b8d2b6e5dc06831a60e1f8248d68b7ab4b2b549d181ec0075920b1883c81caa941c00648ebafac3d23dc56e6fdaa5919b000140503d670486",
        "ip": "localhost",
        "event_id": "1ef7a6f3-3c08-6d1c-888c-0bbb56184a5a",
        "directory": "C:/Users/agata/CBW 2024/ARENA INTEGRA\u00c7\u00c3O 2024",
        "user_name": "CAMPEONATO SUL-AMERICANO"
    }

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

    atletas_json = get_endpoint_response(headers, 'athlete/1ef7a6f3-3c08-6d1c-888c-0bbb56184a5a?limit=400')

    nome_code_dict = {}

    for item in atletas_json['athletes']['items']:
        nome_code_dict[item['personFullName']] = item['teamAlternateName']

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
            177,  # BRA U23
            160,  # BRA U17
            153,  # BRA U15
            179,  # BRA Sênior
        ],
        "pan": [
            156,  # PAN U20
            155,  # PAN U17
            150,  # PAN SÊNIOR
        ],
        "sula": [
            175,  # SUL-AMERICANO
        ],
    }

    df_list = []

    data = CACHE(cache_file_name='all_rank_arena_data').load_dataframe_from_cache()

    data_filter = data[data['id_evento'].isin(ids['nacional'])]

    save_df(data_filter, "xlsx")
    breakpoint()

    for representatividade in ids:
        if representatividade == "nacional":
            for evento in representatividade:

                resultados_api_df = rank_arena_atleta(evento)

                resultados_api_df['Cpf'] = 0
                resultados_api_df['Uf'] = ''
                resultados_api_df['Uf2'] = ''

                with ThreadPoolExecutor(max_workers=10) as executor:
                    resultados_api_df = list(executor.map(process_row, [row for index, row in resultados_api_df.iterrows()]))

                df_list.append(pd.DataFrame(resultados_api_df))

        elif representatividade == "pan":
            pass

        elif representatividade == 'sula':
            pass

    final = pd.concat(df_list)

    save_df(final, "xlsx")

    def format_events_df(df):

        df['QUANT. DE UF NO EVENTO (numero)'] = df.groupby('id_classe_peso')['Uf'].transform('nunique')

        df['QUANT. DE ESTADOS NA PROVA (nomes dos UF)'] = df.groupby('id_classe_peso')['Uf'].transform(lambda x: ', '.join(x.unique()))

        df = df[df['rank'] <= 6]

        # final['Subcategoria etária'] = np.where(final['audienceName'] == 'Base', 'Principal', 'Iniciante')

        colunas = [
            "EVENTO",  # NACIONAL/INTERNACIONAL ( Especificar se for ranking)
            "MODALIDADE",
            "Data do evento",
            "Local do evento",
            "Representatividade do evento ( Mundial , panamericano, olímpico, internacional, nacional, Sul Americano, Estudantil, base, surdolímpico)",
            "Subcategoria etaria ( Principal, Intermediário ou iniciante)",
            "Prova / forma de disputa",
            "PROVA OLIMPICA ( Se é realizada nas olimpíadas)",
            "Genero (Masculino, Feminino ou Misto)",
            "Classificação",
            "Nome (completo) dos atletas",
            "CPF dos atletas",
            "CLASSE FUNCIONAL (paratletas)",
            "QUANT. DE UF NO EVENTO (numero)",
            "QUANT. DE ESTADOS NA PROVA (nomes dos UF)"
        ]

        Colunas_desejadas = ["EVENTO",
                             "MODALIDADE",
                             "Data do evento",
                             "Local do evento",
                             "Representatividade",
                             "Subcategoria etaria",
                             "Prova",
                             "PROVA OLIMPICA ( Se é realizada nas olimpíadas)",
                             "Genero",
                             "Classificação",
                             "Nome",
                             "CPF",
                             "CLASSE FUNCIONAL",
                             "QUANT. DE UF NO EVENTO (numero)",
                             "QUANT. DE ESTADOS NA PROVA (nomes dos UF)"]

        events_data = CACHE(cache_file_name='dados_eventos_2025').load_dataframe_from_cache

        merge = pd.merge(final,
                         events_data,
                         how='left',
                         left_on='id_evento',
                         right_on='id')

        filtrada = merge[['rank',
                          'fullName',
                          'Cpf',
                          'Uf',
                          'classePeso.sexo',
                          'audienceName',
                          'sportName',
                          'name',
                          'descricao',
                          'DistinctCount',
                          'ufs_concatenados']]

        filtrada.to_excel(r"C:\Users\agata\CBW 2025\BOLSA ATLETA\INDICAÇÃO DE RESULTADOS 2025\{evento}.xlsx")

        print(final)
        print(merge.columns)


if __name__ == '__main__':

    run_main_results()



