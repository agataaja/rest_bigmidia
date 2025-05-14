from api_consultas.funtions_main import *
from cache_manager import *

if __name__ == '__main__':

    def iniciar_cahce():

        CACHE(get_ids_ano_eventos([2023,2025,2024]), 'dados_eventos_2023a2025').save_dataframe_to_cache()
        breakpoint()
        CACHE(main_atletas(), 'main_atlleta').save_dataframe_to_cache()
        CACHE(return_all_ranking_2024(), 'ranking_nacional_2024').save_dataframe_to_cache()
        CACHE(get_ids_ano_eventos([2024]), 'dados_eventos_2024').save_dataframe_to_cache()

        CACHE(rank_arena_all_data(), 'todos_dados_api_rank_arena').save_dataframe_to_cache()
        CACHE(consultar_cpf_all(), 'cpf_id_atlleta').save_dataframe_to_cache()
    iniciar_cahce()

    ids = [139, 140, 141, 142]




    for i in ids:

        print(inscritos_eventos_por_id(i))

    breakpoint()

    # estabelecimentos_logos()

    list1 = []

    lista_eventos_internacionias = [99, 122, 106, 103, 105, 102, 101, 95, 91, 120]

    for i in lista_eventos_internacionias:

        df = rank_arena_atleta(i)
        to = df['customId']

        for i in to:

            list1.append(i)

    fim = []
    for lst in list1:
        if lst not in fim:
            fim.append(lst)

    for i in fim:
        print(int(i))
    # lista_por_equipes()
    # atletas_exercito()

    breakpoint()

    main_estabelecimento()



    info_eventos = get_ids_ano_eventos([2024, 2023, 2022])

    concat_list_rank_arena = []
    concat_list_inscritos = []

    for ids in info_eventos['id'].tolist():

        try:
            concat_list_rank_arena.append(rank_arena_atleta(ids))
        except:
            pass
        try:
            concat_list_inscritos.append(inscritos_eventos_por_id(ids))
        except:
            pass

    df_rank_arena = pd.concat(concat_list_rank_arena)
    df_inscritos = pd.concat(concat_list_inscritos)


    df_atletas = main_atletas()
    df_estabelecimentos = main_estabelecimento()

    # variavel_a_ser_adquirida = teste['id'][teste['descricao'] == 'Campeonato Brasileiro U17'].iloc[0]


