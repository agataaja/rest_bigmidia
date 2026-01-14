from api_consultas.funtions_main import *
from cache_manager import *
from ranking_sheets_db import RankingFetcher


def iniciar_cahce():

    # CACHE(rank_arena_all_data(), 'todos_dados_api_rank_arena').save_dataframe_to_cache()
    # CACHE(main_estabelecimento(), cache_file_name='dados_clubes').save_dataframe_to_cache()
    # CACHE(get_ids_ano_eventos([2025]), 'dados_eventos_2025').save_dataframe_to_cache()
    # CACHE(consultar_cpf_all(), 'cpf_id_nome_atleta').save_dataframe_to_cache()
    # CACHE(main_atletas(), 'main_atleta').save_dataframe_to_cache()
    CACHE(RankingFetcher(2025, 'GERAL').fetch_all(), 'ranking_geral_2025').save_dataframe_to_cache()
    # CACHE(get_ids_ano_eventos([2024]), 'dados_eventos_2024').save_dataframe_to_cache()


if __name__ == '__main__':

    iniciar_cahce()




