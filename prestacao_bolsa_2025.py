from api_consultas.funtions_main import *
from datetime import datetime
import locale
from cache_manager import *


CACHE(cache_file_name='dados_prestacao_2025').save_dataframe_to_cache(pd.read_excel(r''))

