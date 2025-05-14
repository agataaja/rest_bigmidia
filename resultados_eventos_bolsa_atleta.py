from api_consultas.funtions_main import *


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

def pegar_lista_de_viadinhos():

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


    lista_de_viadinhos = {}


    for item in atletas_json['athletes']['items']:

        lista_de_viadinhos[item['personFullName']] = item['teamAlternateName']

    return lista_de_viadinhos





def consulta_pais(nome):

    lista = litaaa

    pais = None

    for chave, valor in lista.items():

        if chave == nome:

            pais = valor

            break

    return pais



def process_row(row):


    id_atleta_do_individuo = row['customId']
    nome_individuo = row['fullName']

    try:
        cpf, uf = consultar_cpf(id_atleta_do_individuo)
        row['Cpf'] = cpf
        row['Uf'] = uf
        #row['Uf2'] = 'BRA'

    except:
        row['Cpf'] = 'estrangeiro'
        row['Uf'] = consulta_pais(nome_individuo)
        row['Uf2'] = consulta_pais(nome_individuo)


    return row


if __name__ == '__main__':


    #litaaa = pegar_lista_de_viadinhos()

    ids = [88]
    df_list = []

    for evento in ids:

        resultados_api_df = rank_arena_atleta(evento)

        resultados_api_df['Cpf'] = 0
        resultados_api_df['Uf'] = ''
        resultados_api_df['Uf2'] = ''

        with ThreadPoolExecutor(max_workers=10) as executor:

            resultados_api_df = list(executor.map(process_row, [row for index, row in resultados_api_df.iterrows()]))

        df_list.append(pd.DataFrame(resultados_api_df))

    final = pd.concat(df_list)

    final['DistinctCount'] = final.groupby('id_classe_peso')['Uf'].transform('nunique')
    # final['ufs_concatenados'] = final.groupby('id_classe_peso')['Uf'].transform(lambda x: ', '.join(x))

    final['ufs_concatenados'] = final.groupby('id_classe_peso')['Uf'].transform(lambda x: ', '.join(x.unique()))

    final = final[final['rank'] <= 6]

    # final['Subcategoria etária'] = np.where(final['audienceName'] == 'Base', 'Principal', 'Iniciante')

    # final = final[final['DistinctCount'] >= 5]

    events_data = get_ids_ano_eventos([2024])

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

    filtrada.to_excel('resultados bra senior.xlsx')

    print(final)
    print(merge.columns)
