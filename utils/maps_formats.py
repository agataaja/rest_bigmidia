import datetime
import pandas as pd
from datetime import datetime


def map_style(var):
    if 'GR' in var:
        return "Greco-Romano"

    elif var == 'FS':
        return 'Livre Masculino'

    else:
        return 'Livre Feminino'


def map_style_sge(var):
    if 'Greco' in var:
        return "GR"
    elif 'Livre - Masc.' in var:
        return 'FS'
    elif 'Livre - Fem.' in var:
        return 'WW'
    else:
        return var


def map_age_group_sge(var):
    if var == 'Sub-15':
        return "u15"
    if var == 'Sub-17':
        return "u17"
    if var == 'Sub-20':
        return "u20"
    if var == 'Sênior':
        return "seniors"
    if '23' in var:
        return "u23"
    else:
        return var

def map_audience_name_by_name(var):

    v = var.lower()

    if 'por equipes' in var and 'u15' in var:
        return 'por equipes base'
    elif any(x in v for x in ['u17', 'sub17', 'sub-17', 'u-17']):
        return 'U17'
    elif any(x in v for x in ['u20', 'sub20', 'sub-20', 'u-20']):
        return 'U20'
    elif any(x in v for x in ['u23', 'sub23', 'sub-23', 'u-23']):
        return 'U23'
    elif any(x in v for x in ['u15', 'sub15', 'sub-15', 'u-15']):
        return 'U15'
    elif 'sênior' in v or 'senior' in v or 'aline silva' in v or 'seniors' in v:
        return 'seniors'
    elif 'circuito' in v:
        return ''
    elif 'infantil' in v:
        return 'inf'
    elif 'veteranos' in v:
        return 'vet'
    elif any(x in v for x in ['u16', 'sub16', 'sub-16', 'u-16']):
        return 'U16'
    else:
        return 'foda em'


def map_subcategoria_etaria(var):

    v = var.lower()

    if any(x in v for x in ['u17', 'sub17', 'sub-17', 'u-17', 'sub 17', 'u 17']):
        return 'Iniciante'
    elif any(x in v for x in ['u20', 'sub20', 'sub-20', 'u-20', 'sub 20', 'u 20']):
        return 'Intermediária'
    elif any(x in v for x in ['u23', 'sub23', 'sub-23', 'u-23', 'sub 23', 'u 23']):
        return 'Intermediária'
    elif any(x in v for x in ['u15', 'sub15', 'sub-15', 'u-15', 'sub 15', 'u 15']):
        return 'Base'
    elif 'sênior' in v or 'senior' in v or 'aline silva' in v or 'seniors' in v:
        return 'Principal'
    else:
        return 'foda em'


def capitalize(var):
    return var.capitalize()


def full_date(var):
    try:

        return datetime.strptime(str(var), "%Y-%m-%d %H:%M:%S").strftime(
            "%d de %B de %Y")
    except:
        return var


def local_formatado(var):
    return var.encode('utf-8').decode('utf-8')


def map_style_extense(var):
    if 'Greco' in var or 'GR' == var or 'Greco-Romano - Masc.' == var:
        return "Greco-Romano"

    elif 'Female' in var or 'WW' == var or 'Estilo Livre - Fem.' == var or 'Women' in var:
        return 'Livre Feminino'

    elif 'Freestyle' in var or 'FS' == var or 'Estilo Livre - Masc.' == var:
        return 'Livre Masculino'

    elif 'Beach Wrestling - Fem.' == var or 'BWF' == var:
        return 'Beach Wrestling Feminino'

    elif 'Beach Wrestling - Masc.' == var or 'BWM' == var:
        return 'Beach Wrestling Masculino'


def parse_date(value):
    """
    Converte qualquer entrada de data para datetime.datetime,
    ou retorna pd.NaT se não for possível converter.
    """
    if pd.isna(value):
        return pd.NaT

    if isinstance(value, datetime):
        return value

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, str):
        # Tenta múltiplos formatos comuns
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue

    return pd.NaT  # Se nada funcionar


def to_inteiro(var):
    try:
        return int(var)
    except Exception as e:
        print('exceção:', e)
        return var


def map_is_tipo_clubes(var):
    if var == 1:
        return 'Confederação'

    elif var == 2:

        return 'Federação'

    elif var == 3:

        return 'Academia (não elegível)'

    elif var == 8:

        return 'Clube Aspirante CBC'
    elif var == 9:

        return 'Clube Integrado CBC'
    elif var == 10:

        return 'Clube Filiado Primário CBC'
    elif var == 11:

        return 'Clube Filiado Pleno CBC'
    elif var == 12:

        return 'Clube não integrado CBC (elegível) '
    elif var == 13:

        return 'Clube Novo no Sistema'


def map_by_region(df, uf_col_name):

    region_mapping = {
        'AC': 'Norte',
        'AL': 'Nordeste',
        'AP': 'Norte',
        'AM': 'Norte',
        'BA': 'Nordeste',
        'CE': 'Nordeste',
        'DF': 'Centro-Oeste',
        'ES': 'Sudeste',
        'GO': 'Centro-Oeste',
        'MA': 'Nordeste',
        'MT': 'Centro-Oeste',
        'MS': 'Centro-Oeste',
        'MG': 'Sudeste',
        'PA': 'Norte',
        'PB': 'Nordeste',
        'PR': 'Sul',
        'PE': 'Nordeste',
        'PI': 'Nordeste',
        'RJ': 'Sudeste',
        'RN': 'Nordeste',
        'RS': 'Sul',
        'RO': 'Norte',
        'RR': 'Norte',
        'SC': 'Sul',
        'SP': 'Sudeste',
        'SE': 'Nordeste',
        'TO': 'Norte'
    }

    df['region'] = df[uf_col_name].map(region_mapping)
    return df


def format_cpf(var):

    cpf = str(var).zfill(11)  # Garante que tenha 11 dígitos

    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"