from api_consultas.funtions_main import *
from datetime import datetime
import locale
from cache_manager import *

headers = {}
arquivo_indicacoes_1 = r"C:\Users\agata\CBW 2025\BOLSA ATLETA\INDICAÇÕES BOLSA ATLETA CBW 2025.xlsx"
locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')


def return_id_classe_peso_for_evento(arquivo_indicacoes):

    df_indicacoes = pd.read_excel(arquivo_indicacoes)

    df_indicacoes['PESO'] = [str(values['PESO']) for index, values in df_indicacoes.iterrows()]

    df_indicacoes['concat_values'] = df_indicacoes['IDADE'] + df_indicacoes['ESTILO'] + df_indicacoes['PESO']

    url_lista_peso = f"https://www.cbw.org.br/api/evento-atleta/rank-lista-pesos"

    response_pesos = requests.get(url_lista_peso).json()['items']['2024']['GERAL']

    dfs = []

    estilo_map = {'Estilo Livre - Fem.': 'WW', 'Estilo Livre - Masc.': 'FS', 'Greco-Romano - Masc.': 'GR'}

    for estilo, age_groups in response_pesos.items():

        for age_group, weights in age_groups.items():
            df = pd.json_normalize(weights)

            df['estilo'] = estilo

            df['age_group'] = age_group

            df['style'] = df['estilo'].map(estilo_map)

            dfs.append(df)

    df_classe_id = pd.concat(dfs, ignore_index=True)

    df_classe_id['weight'] = [str(values['peso']).replace("kg", "") for index, values in df_classe_id.iterrows()]
    df_classe_id['concat_values'] = df_classe_id['age_group'] + df_classe_id['style'] + df_classe_id['weight']

    merged = pd.merge(df_indicacoes,
                      df_classe_id,
                      how='left',
                      on='concat_values')

    desired_columns = ['id_classe_peso', 'id_evento']

    existing_columns = [col for col in desired_columns if col in merged.columns]

    return merged.filter(items=existing_columns)


def return_results_formated_eventos(df_eventos_e_classe):

    eventos_infos = CACHE(cache_file_name='dados_eventos_2024').load_dataframe_from_cache()
    results_df = CACHE(cache_file_name='todos_dados_api_rank_arena').load_dataframe_from_cache()
    cpf_info = CACHE(cache_file_name='cpf_id_atleta').load_dataframe_from_cache()

    filtered_results = results_df[results_df['rank'] <= 3]

    merged_results = filtered_results.merge(
                        df_eventos_e_classe,
                        on=['id_classe_peso', 'id_evento'],
                        how='inner'
                    )

    merged_events_results = merged_results.merge(eventos_infos, left_on='id_evento', right_on='id', how='left')

    merged_events_results = merged_events_results.merge(cpf_info, left_on='atleta.id', right_on='id_atleta', how='inner')

    def map_style(var):
        if 'GR' in var:
            return "Greco-Romano"

        elif var == 'FS':
            return 'Livre Masculino'

        else:
            return 'Livre Feminino'

    def full_date(var):

        return datetime.strptime(str(var), "%Y-%m-%d %H:%M:%S").strftime(
            "%d de %B de %Y")

    def local_formatado(var):

        var = var.replace("Ã§", "ç")

        return var.encode('utf-8').decode('utf-8')

    def escopo(row):
        if '15' in row['audienceName']:  # Verifica se '15' está no nome
            row['escopo'] = 'Base'  # Altera o valor da coluna 'escopo' para 'Base'
        return row



    merged_events_results['tipo'] = 'atleta'
    merged_events_results['estilo'] = merged_events_results['sportAlternateName'].apply(map_style)
    merged_events_results['data'] = merged_events_results['data_fim'].apply(full_date)
    merged_events_results['local'] = merged_events_results['local'].apply(local_formatado)
    merged_events_results['cpf'] = merged_events_results['cpf'].apply(format_cpf)
    merged_events_results = merged_events_results.apply(escopo, axis=1)

    desired_columns = ['customId', 'tipo', 'fullName', 'rank', 'cpf', 'descricao', 'local', 'data', 'audienceName',  'estilo', 'name', 'escopo']

    df = merged_events_results.filter(items=desired_columns)

    final = df.rename(columns={"customId": "id", "fullName": "nome_completo", "descricao": "evento", "audienceName": "classe", 'name': 'prova'})

    #final.to_csv(r"C:\Users\agata\CBW 2025\BOLSA ATLETA\EVENTOS.csv", index=False)

    print(final[final['evento'] == 'Campeonato Sul-Americano de Wrestling 2024'])

    return final


def return_results_formated_ranking(df_eventos_e_classe):

    eventos_infos = CACHE(cache_file_name='dados_eventos_2024').load_dataframe_from_cache()

    results_df = CACHE(cache_file_name='todos_dados_api_rank_arena').load_dataframe_from_cache()
    cpf_info = CACHE(cache_file_name='cpf_id_atleta').load_dataframe_from_cache()
    ranking_nacional_2024 = CACHE(cache_file_name='ranking_nacional_2024').load_dataframe_from_cache()

    ranking_nacional_2024 = ranking_nacional_2024[ranking_nacional_2024['colocacao'] <= 3]

    df_eventos_e_classe = df_eventos_e_classe[df_eventos_e_classe['id_evento'] == 'none']

    eventos_infos = eventos_infos[eventos_infos['escopo'] == 'Nacional']

    eventos_infos = eventos_infos[~eventos_infos['id'].isin([109, 110, 111, 125, 126, 127, 128, 130])]

    ranking_indicado = df_eventos_e_classe.merge(ranking_nacional_2024, on=['id_classe_peso'], how='left')

    return_df = ranking_indicado.merge(results_df, left_on=['id_classe_peso', 'id_atleta'], right_on=['id_classe_peso', 'atleta.id'], how='inner')

    merged_events_results = return_df.merge(eventos_infos, left_on='id_evento_y', right_on='id', how='inner')

    merged_events_results = merged_events_results.merge(cpf_info, left_on='atleta.id', right_on='id_atleta',
                                                        how='left')

    merged_events_results['tipo'] = 'atleta'
    merged_events_results['ranking'] = 'Ranking Nacional de Wrestling 2024'
    merged_events_results['estilo'] = merged_events_results['sportAlternateName'].apply(map_style)
    merged_events_results['data'] = merged_events_results['data_fim'].apply(full_date)
    merged_events_results['local'] = merged_events_results['local'].apply(local_formatado)
    merged_events_results['cpf'] = merged_events_results['cpf'].apply(format_cpf)

    desired_columns_2 = ['customId', 'ranking', 'escopo', 'tipo', 'fullName', 'colocacao', 'cpf', 'audienceName', 'estilo', 'name', 'id_classe_peso']

    nunique_df = merged_events_results.filter(items=desired_columns_2)

    nunique_df = nunique_df.drop_duplicates(subset=['customId', 'id_classe_peso'], keep='first')

    def make_events_list(id_atleta, id_classe_peso):

        filter_df_2 = merged_events_results[
            (merged_events_results['id_classe_peso'] == id_classe_peso) &
            (merged_events_results['customId'] == id_atleta)
            ]
        results_text = []

        for i, lineee in filter_df_2.iterrows():

            rank = lineee['rank']
            prova = lineee['name']
            estilo = lineee['estilo']
            classe = lineee['audienceName']
            evento = lineee['descricao']
            data = lineee['data']
            local = lineee['local']
            text = f'<li>{rank}ª classificação na prova {prova}, no estilo {estilo}, na classe {classe}, no {evento}, realizado no(s) dia(s) {data}, na cidade de {local}.</li>'
            text = text.replace("Ã§", "ç")
            results_text.append(text)
        print(f'<ul style="font-family: Calibri, sans-serif; font-size: 10pt;">{"".join(results_text)}</ul>')

        return f'<p><ul style="font-family: Calibri, sans-serif; font-size: 10pt;">{"".join(results_text)}</ul></p>'

    nunique_df['resultados_compilados'] = nunique_df.apply(lambda linha: make_events_list(linha['customId'], linha['id_classe_peso']), axis=1)

    final = nunique_df.rename(
        columns={"customId": "id", "fullName": "nome_completo", "audienceName": "classe",
                 'name': 'prova', 'colocacao': 'rank'})

    import csv
    final.to_csv(r"C:\Users\agata\CBW 2025\BOLSA ATLETA\RANKING.csv",
                 index=False,
                 sep=";",
                 quoting=csv.QUOTE_MINIMAL)

    return final


if __name__ == '__main__':


    return_results_formated_eventos(return_id_classe_peso_for_evento(arquivo_indicacoes_1))
    #return_results_formated_ranking(return_id_classe_peso_for_evento(arquivo_indicacoes_1))
