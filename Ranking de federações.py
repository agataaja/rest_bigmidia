import pandas as pd


respostas_evidencias_df = pd.read_excel(
    r"C:\Users\agata\CBW 2024\Evidencias Programa de Diagnóstico de Federações 2024\Ranking de Federações - CBW.xlsx",
    sheet_name='RELÇÃO EVIDENCIAS')


intervalos_por_area = {'administrativa': (3, 17),
                       'técnica': (16, 43),
                       'educação': (44, 48),
                       'eventos': (49, 53),
                       'fomento': (54, 59)
                       }

colunas_por_area = {
    'administrativa': [
        "1. Atualização de dados da Federação [1.1. Contém Estatuto]",
        "1. Atualização de dados da Federação [1.7 Contém Ata de Eleição atualizada]",
        "1. Atualização de dados da Federação [1.2. Apresenta prestação de contas reconhecida em cartório]",
        "1. Atualização de dados da Federação [1.3. Apresenta descrição de organograma e mapa de diretoria]",
        "1. Atualização de dados da Federação [1.4. Apresenta calendário de eventos atualizado]",
        "1. Atualização de dados da Federação [1.5. Apresenta resultados oficiais e/ou relatórios de eventos]",
        "1. Atualização de dados da Federação [1.6. Mantém locais de prática atualizados (onde treinar)]",
        "*1. Atualização de dados da Federação (upload de evidências em PDF)",
        "2.1 Quantidade de Atletas e Treinadores filiados com anuidade em dia com a CBW [Atletas]",
        "2.1 Quantidade de Atletas e Treinadores filiados com anuidade em dia com a CBW [Treinadores]",
        "2.2 Quantidade Clubes e Associações filiados a CBW",
        "3. Quantidade de atletas filiados com cadastro atualizado no sistema de gestão esportiva da CBW",
        "4. Comparecimento em Assembléias da CBW",
        "5. Atualização de dados da Federação dentro do Sistema da CBW (BIGMIDIA)"
    ],
    'técnica': [
        "1. Possui Seleção Estadual",
        "*1. Possui Seleção Estadual (upload de evidências)",
        "2.1 Número de atletas que compõem a Seleção Brasileira SUB-17 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE MASCULINO]",
        "2.1 Número de atletas que compõem a Seleção Brasileira SUB-17 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE FEMININO]",
        "2.1 Número de atletas que compõem a Seleção Brasileira SUB-17 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [GRECO-ROMANA]",
        "2.2 Número de atletas que compõem a Seleção Brasileira SUB-20 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE MASCULINO]",
        "2.2 Número de atletas que compõem a Seleção Brasileira SUB-20 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE FEMININO]",
        "2.2 Número de atletas que compõem a Seleção Brasileira SUB-20 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [GRECO-ROMANA]",
        "2.3 Número de atletas que compõem a Seleção Brasileira Sênior 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE MASCULINO]",
        "2.3 Número de atletas que compõem a Seleção Brasileira Sênior 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [LIVRE FEMININO]",
        "2.3 Número de atletas que compõem a Seleção Brasileira Sênior 2023 (convocação oficial para participação em mundial, campeonato continental e jogos) [GRECO-ROMANA]",
        "3.1 Quantidade de atletas SUB-15 entre os TOP 05 no ranking GERAL 2023 [LIVRE MASCULINO]",
        "3.1 Quantidade de atletas SUB-15 entre os TOP 05 no ranking GERAL 2023 [LIVRE FEMININO]",
        "3.1 Quantidade de atletas SUB-15 entre os TOP 05 no ranking GERAL 2023 [GRECO-ROMANA]",
        "3.2 Quantidade de atletas SUB-17 entre os TOP 05 no ranking GERAL 2023 [LIVRE MASCULINO]",
        "3.2 Quantidade de atletas SUB-17 entre os TOP 05 no ranking GERAL 2023 [LIVRE FEMININO]",
        "3.2 Quantidade de atletas SUB-17 entre os TOP 05 no ranking GERAL 2023 [GRECO-ROMANA]",
        "3.3 Quantidade de atletas SUB-20 entre os TOP 05 no ranking GERAL 2023 [LIVRE MASCULINO]",
        "3.3 Quantidade de atletas SUB-20 entre os TOP 05 no ranking GERAL 2023 [LIVRE FEMININO]",
        "3.3 Quantidade de atletas SUB-20 entre os TOP 05 no ranking GERAL 2023 [GRECO-ROMANA]",
        "3.4 Quantidade de atletas Seniors entre os TOP 05 no ranking GERAL 2023 [LIVRE MASCULINO]",
        "3.4 Quantidade de atletas Seniors entre os TOP 05 no ranking GERAL 2023 [LIVRE FEMININO]",
        "3.4 Quantidade de atletas Seniors entre os TOP 05 no ranking GERAL 2023 [GRECO-ROMANA]",
        "4.1 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-15 2023 [LIVRE MASCULINO]",
        "4.1 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-15 2023 [LIVRE FEMININO]",
        "4.1 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-15 2023 [GRECO ROMANO]",
        "4.2 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-17 2023 [LIVRE MASCULINO]",
        "4.2 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-17 2023 [LIVRE FEMININO]",
        "4.2 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-17 2023 [GRECO ROMANO]",
        "4.3 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-20 2023 [LIVRE MASCULINO]",
        "4.3 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-20 2023 [LIVRE FEMININO]",
        "4.3 Quantidade de atletas enviados ao Campeonato Brasileiro SUB-20 2023 [GRECO ROMANO]",
        "4.4 Quantidade de atletas enviados ao Campeonato Brasileiro Sênior 2023 [LIVRE MASCULINO]",
        "4.4 Quantidade de atletas enviados ao Campeonato Brasileiro Sênior 2023 [LIVRE FEMININO]",
        "4.4 Quantidade de atletas enviados ao Campeonato Brasileiro Sênior 2023 [GRECO ROMANO]",
        "5. Número de árbitros Nacionais e Internacionais [Nacionais]",
        "5. Número de árbitros Nacionais e Internacionais [Internacionais]"
    ],
    'educação': [
        "1. Ofereceu Curso/Workshop/Palestra",
        "2. Ofereceu Curso/Workshop de arbitragem",
        "3. Ofereceu Curso/Workshop de treinadores/professores",
        "4. Ofereceu Curso/Workshop com treinador externo",
        "* Upload de Evidências Área de Desenvolvimento (em PDF)",
    ],
    'eventos': [
        "1. Organizou Campeonato Estadual",
        "*1. Organizou Campeonato Estadual (upload de evidências EM PDF)",
        "2. Quantidade de competições nacionais organizadas",
        "3. Organizou Festival Infantil de Wrestling",
        "*3. Organizou Festival Infantil de Wrestling (upload de evidências EM PDF)",
    ],
    'fomento': [
        "1. Faz parte de eventos Multiesportivos do Estado",
        "2. Possui algum projeto de Lei de Incentivo aprovado e captado [Aprovado]",
        "2. Possui algum projeto de Lei de Incentivo aprovado e captado [Captado]",
        "3. Possui Patrocínio Privado",
        "4. Quantidade de projetos sociais dentro do Estado",
        "* Upload de Evidências Área de Fomento ao Esporte em PDF"
    ],
                       }
pontuacao_geral_por_area = {'administrativa': 450,
                            'técnica': 450,
                            'educação': 150,
                            'eventos': 300,
                            'fomento': 150}






print(len(colunas_por_area['administrativa']))
#print(respostas_evidencias_df.columns)

