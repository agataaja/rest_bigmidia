from resultados_eventos_bolsa_atleta import consultar_cpf
from api_consultas.funtions_main import *


def return_rank_for_class_id():

    headers = {}
    headers = []

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

            if age_group == 'Sub-17' or age_group == 'Sub-20':

                dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    print(result_df)
    final_df =[]

    for index, row in result_df.iterrows():

        url_rank_category = f"https://www.cbw.org.br/api/evento-atleta/rank-atual?sort=colocacao&ano=2024&grupo=NACIONAL&id_classe_peso="
        querys = f'{row["id_classe_peso"]}'
        page_count = requests.get(f"{url_rank_category}{querys}").json()["_meta"]["page_count"]
        # response_ranking = requests.get(url_rank_category).json()['items']

        with ThreadPoolExecutor() as executor:
            dfs1 = executor.map(lambda page: fetch_data(url_rank_category, querys, headers, page),
                               range(1, page_count + 1))
        try:
            final = pd.concat(dfs1, ignore_index=True)
            final_df.append(final)
        except:

            final = pd.DataFrame(dfs1)
            final_df.append(final)

    final_df = pd.concat(final_df,  ignore_index=True)

    final_df['CPF'] = None

    for index, row in final_df.iterrows():

        cpf, uf = consultar_cpf(row['id_atleta'])
        final_df.loc[index, 'CPF'] = cpf


    final_df['DistinctCount'] = final_df.groupby('id_classe_peso')['federacao_uf'].transform('nunique')
    # final['ufs_concatenados'] = final.groupby('id_classe_peso')['Uf'].transform(lambda x: ', '.join(x))

    final_df['ufs_concatenados'] = final_df.groupby('id_classe_peso')['federacao_uf'].transform(lambda x: ', '.join(x.unique()))

    final_df = final_df[final_df['colocacao'] <= 6]


    final_df.to_excel('ranking u17-u20 teste.xlsx')

    return final_df


if __name__ == '__main__':


    print(return_rank_for_class_id())
