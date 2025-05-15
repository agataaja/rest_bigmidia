from api_consultas.funtions_main import *
from cache_manager import *

if __name__ == '__main__':

    def iniciar_cahce():

        CACHE(rank_arena_all_data(), 'todos_dados_api_rank_arena').save_dataframe_to_cache()

        CACHE(get_ids_ano_eventos([2023, 2025, 2024]), 'dados_eventos_2023a2025').save_dataframe_to_cache()

        # CACHE(consultar_cpf_all(), 'cpf_id_nome_atleta').save_dataframe_to_cache()
        # CACHE(main_atletas(), 'main_atlleta').save_dataframe_to_cache()
        # CACHE(return_all_ranking_2024(), 'ranking_nacional_2024').save_dataframe_to_cache()
        # CACHE(get_ids_ano_eventos([2024]), 'dados_eventos_2024').save_dataframe_to_cache()

    iniciar_cahce()


