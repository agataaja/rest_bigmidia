import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional, Any, Dict
from utils.maps_formats import parse_date  # mantém seu parser existente
import logging
from pathlib import Path
import json
from cache_manager import CACHE


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DEFAULT_HEADERS = {"Content-Type": "application/json"}


def _safe_join_expand(expands: Optional[List[str]]) -> str:
    if not expands:
        return ""
    # aceita entradas já como "a,b" ou ["a", "b"]
    if len(expands) == 1 and "," in expands[0]:
        return expands[0]
    return ",".join(expands)


def _get_page_count_from_meta(resp_json: Dict[str, Any]) -> int:
    meta = resp_json.get("_meta") or {}
    return int(meta.get("pageCount", 1))


def _ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    # algumas APIs retornam {"item": [...]} em vez de lista direta — lidar depois
    return [x]


class BaseCBWApi:
    """
    Base com funcionalidades comuns (session, fetch paginado, flatten helpers).
    Não deve ser instanciada diretamente.
    """
    BASE_URL: str = ""  # override in subclasses

    def __init__(self, access_token: str, max_workers: int = 8, headers: Optional[Dict[str,str]] = None):
        self.access_token = access_token
        self.max_workers = max_workers
        self.session = requests.Session()
        self.headers = headers or DEFAULT_HEADERS.copy()

    def _build_query(self, expands: Optional[List[str]] = None, extra_params: Optional[Dict[str, str]] = None) -> str:
        parts = []
        if expands:
            expand_str = _safe_join_expand(expands)
            parts.append(f"expand={expand_str}")
        if extra_params:
            for k, v in extra_params.items():
                parts.append(f"{k}={v}")
        parts.append(f"access-token={self.access_token}")
        query = "?" + "&".join(parts) if parts else ""
        return query

    def _request_json(self, url: str) -> Dict[str, Any]:
        r = self.session.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def _fetch_page_items(self, endpoint_url: str, page: int) -> List[Dict[str,Any]]:
        url = f"{endpoint_url}&page={page}"
        resp_json = self._request_json(url)
        # Alguns endpoints retornam 'items' e outros podem devolver lista direta; lidar com isso
        if isinstance(resp_json, dict) and "items" in resp_json:
            return resp_json.get("items", [])
        if isinstance(resp_json, list):
            return resp_json
        # fallback: procurar por chave 'data' ou 'results'
        return resp_json.get("data", resp_json.get("results", []))

    def _fetch_paginated_items(self, endpoint_path: str, expands: Optional[List[str]] = None,
                               extra_params: Optional[Dict[str, str]] = None) -> List[Dict[str,Any]]:
        query = self._build_query(expands=expands, extra_params=extra_params)
        full_url = f"{self.BASE_URL}/{endpoint_path}{query}"
        # pegar meta (pageCount)
        resp_json = self._request_json(full_url)
        page_count = _get_page_count_from_meta(resp_json)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            pages_iter = executor.map(lambda p: self._fetch_page_items(full_url, p), range(1, page_count + 1))

        # concatenar listas de páginas
        items = []
        for page_items in pages_iter:
            items.extend(page_items or [])
        return items

    @staticmethod
    def extract_cpf_from_documentos(atleta_documentos: Any) -> Optional[str]:
        """
        Extrai o documento com id_tipo == 1 (CPF). Trata: None, lista direta, dict {"item": [...]}
        """
        if atleta_documentos is None:
            return None

        # API às vezes devolve {'item': [ ... ]} ou lista direta
        if isinstance(atleta_documentos, dict) and "item" in atleta_documentos:
            docs = atleta_documentos.get("item", [])
        else:
            docs = atleta_documentos

        if not isinstance(docs, list):
            # se for um único objeto
            docs = [docs]

        for doc in docs:
            # alguns campos podem faltar; .get para não explodir
            if doc.get("id_tipo") == 1:
                numero = doc.get("numero")
                return numero if numero not in ("", None) else None
        return None

    @staticmethod
    def json_normalize_items(items: List[Dict[str, Any]]) -> pd.DataFrame:
        if not items:
            return pd.DataFrame()
        return pd.json_normalize(items)


class CBWGestaoAPI(BaseCBWApi):
    """
    Classe para endpoints sob:
    https://restcbw.bigmidia.com/gestao/api
    """

    BASE_URL = "https://restcbw.bigmidia.com/gestao/api"

    # ------------------------------
    # Endpoints públicos (equivalentes às suas funções)
    # ------------------------------

    def get_atletas(self, expands: Optional[List[str]] = None, flag_del: int = 0) -> pd.DataFrame:
        """
        Similar a main_atletas()
        """
        if expands is None:
            expands = ["atletaDocumentos", "estabelecimento"]
        extra = {"flag_del": str(flag_del)}
        items = self._fetch_paginated_items("atleta", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)

        # Transformação que você aplicou: data_create timestamp -> dd-mm-YYYY
        if "data_create" in df.columns:
            def _ts_to_str(x):
                try:
                    if x in (None, "", 0):
                        return None
                    return datetime.fromtimestamp(int(x)).strftime("%d-%m-%Y")
                except Exception:
                    return x
            df['data_create'] = df['data_create'].apply(_ts_to_str)
        return df

    def get_pessoas(self, expands: Optional[List[str]] = None, flag_del: int = 0) -> pd.DataFrame:
        """
        Similar a main_pessoas()
        """
        if expands is None:
            expands = ["atletaDocumentos", "estabelecimento"]
        extra = {"flag_del": str(flag_del)}
        items = self._fetch_paginated_items("pessoa", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)
        return df

    def get_estabelecimentos(self, expands: Optional[List[str]] = None, flag_del: int = 0) -> pd.DataFrame:
        """
        Similar a main_estabelecimento()
        - Filtra tipos de estabelecimento (remove tipo 1 e 2)
        - Aplica parse_date em created_at (como antes)
        """
        if expands is None:
            expands = []
        extra = {"flag_del": str(flag_del)}
        items = self._fetch_paginated_items("estabelecimento", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)
        if df.empty:
            return df

        # filtra ~id_estabelecimento_tipo in [1,2]
        if 'id_estabelecimento_tipo' in df.columns:
            filtered_df = df[~df['id_estabelecimento_tipo'].isin([1, 2])]
        else:
            filtered_df = df

        # parse created_at se existir
        if 'created_at' in filtered_df.columns:
            filtered_df['created_at'] = filtered_df['created_at'].apply(lambda x: parse_date(x) if x else x)

        return filtered_df

    def get_ids_ano_eventos(self, anos: List[Any]) -> pd.DataFrame:
        """
        Similar a get_ids_ano_eventos(anos)
        -> recebe lista/iterável de anos (ex.: [2024, 2025] ou ['2025'])
        -> retorna DataFrame com eventos filtrados por ano (checa data_fim contendo o ano)
        """
        extra = {"flag_del": "0"}
        items = self._fetch_paginated_items("evento", expands=None, extra_params=extra)
        df = self.json_normalize_items(items)
        if df.empty:
            return df

        # filtrar por anos (cada entrada de anos como str)
        dfs = []
        anos_str = [str(a) for a in anos]
        for ano in anos_str:
            # cuidado se data_fim não existir
            if 'data_fim' in df.columns:
                mask = df['data_fim'].astype(str).str.contains(ano, na=False)
                dfs.append(df[mask])
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    # ------------------------------
    # CPF helpers
    # ------------------------------

    def consultar_cpf_all(self) -> pd.DataFrame:
        """
        Similar a consultar_cpf_all() - retorna DataFrame com id_atleta, cpf, nome_completo
        """
        expands = ["atletaDocumentos", "estabelecimento"]
        items = self._fetch_paginated_items("atleta", expands=expands, extra_params=None)
        # construir lista de linhas conforme seu fetch_data_cpf
        rows = []
        for item in items:
            id_atleta = item.get("id")
            nome_completo = (item.get("nome_completo") or "").upper()
            cpf = self.extract_cpf_from_documentos(item.get("atletaDocumentos"))
            rows.append({"id_atleta": id_atleta, "cpf": cpf, "nome_completo": nome_completo})
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def consultar_cpf(self, id_atleta: Any) -> Optional[str]:
        """
        Similar a consultar_cpf(id_atleta) - consulta atleta específico
        """
        expands = ["atletaDocumentos", "estabelecimento"]
        query = self._build_query(expands=expands, extra_params=None)
        full_url = f"{self.BASE_URL}/atleta/{id_atleta}{query}"
        resp_json = self._request_json(full_url)
        # resp_json é o objeto do atleta
        cpf = self.extract_cpf_from_documentos(resp_json.get("atletaDocumentos") or resp_json.get("atleta_documentos"))
        return cpf

    # ------------------------------
    # util: baixar logos de estabelecimentos (estabelecimentos_logos)
    # ------------------------------
    def download_estabelecimentos_logos(self, out_dir: str = ".", replace_domain: str = "sge.cbw.org.br"):
        """
        Faz o equivalente a estabelecimentos_logos(): baixa cada logo transformando domínio.
        Salva arquivos como {id}.png no diretório out_dir.
        """
        df = self.get_estabelecimentos()
        os.makedirs(out_dir, exist_ok=True)
        for idx, row in df.iterrows():
            id_str = str(row.get("id"))
            url = row.get("urlLogo") or row.get("url_logo") or row.get("urlLogo")
            if not url or not isinstance(url, str):
                logger.debug("Sem url para estabelecimento %s", id_str)
                continue
            transformed_url = url.replace("cbw.bigmidia.com", replace_domain)
            try:
                r = self.session.get(transformed_url)
                r.raise_for_status()
                path = os.path.join(out_dir, f"{id_str}.png")
                with open(path, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                logger.warning("Falha ao baixar logo %s: %s", transformed_url, str(e))


class CBWArenaAPI(BaseCBWApi):
    """
    Classe para endpoints sob:
    https://restcbw.bigmidia.com/cbw/api
    """

    BASE_URL = "https://restcbw.bigmidia.com/cbw/api"

    def get_inscritos_eventos_por_id(self, id_evento: Any, expands: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Similar a inscritos_eventos_por_id(id_evento)
        """
        if expands is None:
            expands = ["classe", "modalidade", "classePeso", "atleta", "atletaGestao", "estabelecimento"]
        extra = {"id_evento": str(id_evento)}
        items = self._fetch_paginated_items("evento-atleta", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)
        return df

    def rank_arena_atleta(self, id_evento: Any, expands: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Similar a rank_arena_atleta(id_evento)
        """
        if expands is None:
            expands = ["classe", "modalidade", "classePeso", "atleta", "atletaGestao", "estabelecimento"]
        extra = {"id_evento": str(id_evento)}
        items = self._fetch_paginated_items("resultado-rank-arena", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)
        return df

    def rank_arena_all_data(self, expands: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Similar a rank_arena_all_data()
        """
        # se expand for None, busca sem expand
        extra = {}
        items = self._fetch_paginated_items("resultado-rank-arena", expands=expands, extra_params=extra)
        df = self.json_normalize_items(items)
        return df

    def rank_arena_atleta_resultados(self, ano_list: List[Any], atletas_df: pd.DataFrame, gestao_api: CBWGestaoAPI) -> pd.DataFrame:
        """
        Reimplementa rank_arena_atleta_resultados(ano, df) usando:
        - anos (lista)
        - atletas_df: DataFrame com coluna "id" (naturalmente vindo do CBWGestaoAPI.get_atletas ou similar)
        - gestao_api: instância CBWGestaoAPI para obter eventos do ano
        Retorna DataFrame dos resultados individuais dos atletas passados.
        """
        # obter eventos do ano via gestao_api
        events_data = gestao_api.get_ids_ano_eventos(ano_list)
        if events_data.empty:
            return pd.DataFrame()

        concat_list = []
        for evento_id in events_data['id'].tolist():
            try:
                df100 = self.rank_arena_atleta(evento_id)
                merge = pd.merge(df100,
                                 events_data,
                                 how='left',
                                 left_on='id_evento',
                                 right_on='id')
                concat_list.append(merge)
            except Exception as e:
                logger.debug("Erro ao processar evento %s: %s", evento_id, str(e))
                continue

        if not concat_list:
            return pd.DataFrame()
        df_global = pd.concat(concat_list, ignore_index=True)

        # para cada atleta no atletas_df, filtrar por customId == str(id)
        individual_results_list = []
        if 'id' not in atletas_df.columns:
            return pd.DataFrame()  # sem coluna id, nada a fazer

        for _id in atletas_df["id"]:
            filtered_df = df_global[df_global['customId'] == str(_id)]
            individual_results_list.append(filtered_df)

        if not individual_results_list:
            return pd.DataFrame()
        individual_results_df = pd.concat(individual_results_list, ignore_index=True)
        return individual_results_df


if __name__ == "__main__":

    BASE_DIR = Path(__file__).resolve().parent.parent
    KEY_FILE = os.path.join(BASE_DIR, r'static\json\token_sge.json')

    with open(KEY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    TOKEN = data["access-token"]

    gestao = CBWGestaoAPI(access_token=TOKEN)
    arena = CBWArenaAPI(access_token=TOKEN)

    df_all_rank = arena.rank_arena_all_data(expands=['atleta', 'atletaGestao', 'estabelecimento', 'atletaDocumentos'], )

    CACHE(df_all_rank, cache_file_name="all_rank_arena_data").save_dataframe_to_cache()


    # Exemplos rápidos (comente/descomente conforme quiser testar)
    # df_atletas = gestao.get_atletas()
    # df_pessoas = gestao.get_pessoas()
    # df_estab = gestao.get_estabelecimentos()
    # df_cpfs = gestao.consultar_cpf_all()
    # cpf_single = gestao.consultar_cpf(1399)
    # df_inscritos = arena.get_inscritos_eventos_por_id(49)
    # df_rank = arena.rank_arena_atleta(49)
    # df_all_rank = arena.rank_arena_all_data()
    # arena.download_estabelecimentos_logos()  # não faz parte da arena, mas você pode usar gestao.download_estabelecimentos_logos()

    logger.info("CBW API module carregado com sucesso.")
