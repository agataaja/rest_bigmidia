from api_consultas.funtions_main import *
from connections.sheets import *
from utils.maps_formats import *


class RankingFetcher:
    def __init__(self, ano, escopo):
        self.ano = ano
        self.escopo = str(escopo)
        self.headers = {"Content-Type": "application/json"}
        self.base_url = "https://www.cbw.org.br/api/evento-atleta/"
        self.estilo_map = {
            'Estilo Livre - Fem.': 'WW',
            'Estilo Livre - Masc.': 'FS',
            'Greco-Romano - Masc.': 'GR',
            'Beach Wrestling - Fem.': 'BWF',
            'Beach Wrestling - Masc.': 'BWM'
        }
        self.idade_map = {
            'Infantil 11 e 12': 'inf 11-12',
            'Sênior': 'seniors',
            'Sub-15': 'u15',
            'Sub-17': 'u17',
            'Sub-20': 'u20',
            'Veteranos A': 'veterans-a'
        }

    def fetch_all(self):
        pesos_df = self._get_id_classe_peso()
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(
                lambda row: self._fetch_ranking_for_peso(row['id_classe_peso']),
                pesos_df.to_dict('records')
            ))

        print('problema antes daqui')
        final_df = pd.concat(results, ignore_index=True)
        return final_df

    def _get_id_classe_peso(self):
        url = f"{self.base_url}rank-lista-pesos"
        response = requests.get(url).json()['items'][str(self.ano)][self.escopo]
        rows = []

        for estilo, grupos in response.items():
            for grupo, pesos in grupos.items():
                for peso_info in pesos:
                    rows.append({
                        'style': self.estilo_map.get(estilo, estilo),
                        'age_group': self.idade_map.get(grupo, grupo),
                        'id_classe_peso': peso_info['id_classe_peso'],
                        'peso': peso_info['peso']
                    })
        return pd.DataFrame(rows).drop_duplicates()

    def _fetch_ranking_for_peso(self, id_classe_peso):
        url = f"{self.base_url}rank-atual?sort=colocacao&ano={self.ano}&grupo={self.escopo}&id_classe_peso={id_classe_peso}"
        try:
            page_count = requests.get(url).json()["_meta"]["page_count"]
        except Exception as e:
            print(f"Erro ao obter páginas para {id_classe_peso}: {e}")

            return pd.DataFrame()

        with ThreadPoolExecutor() as executor:
            pages = list(executor.map(lambda p: self._fetch_page(url, p), range(1, page_count + 1)))

        df = pd.concat([p for p in pages if not p.empty], ignore_index=True)
        if not df.empty:
            df['escopo'] = self.escopo
            df['ano'] = self.ano
            df['estilo'] = df['estilo'].apply(map_style_extense)
            df['escopo'] = df['escopo'].apply(capitalize)
        return df

    def _fetch_page(self, url, page):
        try:
            resp = requests.get(f"{url}&page={page}", headers=self.headers)
            items = resp.json().get("items", [])
            return pd.DataFrame(items)
        except Exception as e:
            print(f"Erro ao buscar página {page}: {e}")
            return pd.DataFrame()


if __name__ == '__main__':

    sheet = setup_google_sheet('REST API BIGMIDIA', 'RANKS')

    anos = [2023, 2024, 2025]
    escopos = ['GERAL', 'NACIONAL']
    ano_atual = datetime.now().year

    dfs_list = []
    for ano in anos:
        for escopo in escopos:

            fetcher = RankingFetcher(ano, escopo)
            df = fetcher.fetch_all()

            print(df)

            if ano < ano_atual:

                df['data_recorte'] = parse_date(f'31/12/{ano}')
            else:
                df['data_recorte'] = parse_date(datetime.now())

            dfs_list.append(df)
            print('done:', ano, escopo)


    final_df = pd.concat(dfs_list, ignore_index=True)

    final_df = final_df.astype(str)

    # Limpa a planilha antes de postar os dados
    sheet.clear()

    # Prepara os dados para upload: cabeçalhos + dados
    values = [final_df.columns.tolist()] + final_df.values.tolist()

    # Atualiza a planilha com todos os dados de uma vez
    sheet.update(values)




