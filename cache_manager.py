import joblib
import os
import pandas as pd

CACHE_DIR = 'cache'


class CACHE:

    def __init__(self, cache_instance=None, cache_file_name=None):

        self.object = cache_instance
        self.file_name = os.path.join(
            CACHE_DIR, f"{cache_file_name}.pkl"
        ) if cache_file_name else None

    def to_df(self, cache_instance):

        if isinstance(cache_instance, dict):  # Verifica se é um JSON (representado como dict em Python)
            print("Recebi um JSON!")
            df = pd.json_normalize(cache_instance)
            return df

        elif isinstance(cache_instance, list):  # Verifica se é um JSON (representado como dict em Python)
            print("Recebi uma Lista!")
            df = pd.DataFrame(cache_instance)
            return df

        elif isinstance(cache_instance, str) and cache_instance.endswith('.txt'):  # Verifica se é um arquivo de texto
            print("Recebi um arquivo TXT!")
            # Lógica para processar arquivo TXT

        elif isinstance(cache_instance, str) and cache_instance.endswith('.csv'):  # Verifica se é um arquivo CSV
            print("Recebi um arquivo CSV!")

        elif isinstance(cache_instance, pd.DataFrame):
            return cache_instance

        else:
            print("Tipo de dado não suportado.")

            raise ValueError("Formato de arquivo não reconhecido.")

    def save_dataframe_to_cache(self, cache_instance=None):
        """
        Save a DataFrame to the cache.

        :param cache_instance: The object to save (optional; defaults to `self.object`).
        """
        cache_instance = self.object

        #if not cache_instance:
            #raise ValueError("No object provided to save to cache.")
        if not self.file_name:
            raise ValueError("Cache file name is not set.")

        df = cache_instance

        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
        joblib.dump(df, self.file_name)

    def load_dataframe_from_cache(self):
        """
        Load a DataFrame from the cache if it exists.

        :return: The cached DataFrame or None if no cache exists.
        """
        if not self.file_name:
            raise ValueError("Cache file name is not set.")
        if os.path.exists(self.file_name):
            return joblib.load(self.file_name)
        return None

    def clear_cache(self):
        """
        Clear the cache by deleting the cache file if it exists.
        """
        if not self.file_name:
            raise ValueError("Cache file name is not set.")
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
            print(f"Cache cleared: {self.file_name}")
        else:
            print("No cache file to clear.")

    def atualizar_todo_cache(self):

        if not os.path.exists(CACHE_DIR):
            print("No cache directory found.")
            return

        for file in os.listdir(CACHE_DIR):
            if file.endswith(".pkl"):
                file_path = os.path.join(CACHE_DIR, file)
                print(f"Updating cache: {file}")
                try:
                    # Load the cache file
                    df = joblib.load(file_path)

                    # Save the updated DataFrame back to the file
                    joblib.dump(df, file_path)
                    print(f"Cache updated: {file}")
                except Exception as e:
                    print(f"Failed to update {file}: {e}")








