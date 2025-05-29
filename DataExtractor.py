import fastf1 as ff1
import os
import pandas as pd
import time

class F1DataExtractor:
    def __init__(self, cache_dir='cache'):
        os.makedirs(cache_dir, exist_ok=True)
        ff1.Cache.enable_cache(cache_dir)

    def save_to_csv(self, data, folder_path, file_name):
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)
        if not os.path.exists(file_path):
            try:
                data.to_csv(file_path, index=False)
                print(f"Guardado en: {file_path}")
            except Exception as e:
                print(f"Error guardando {file_path}: {e}")
        else:
            print(f"{file_path} ya existe, se omite.")

    def get_schedule(self, year):
        try:
            return ff1.get_event_schedule(year)
        except Exception as e:
            print(f"Error cargando calendario de {year}: {e}")
            return pd.DataFrame()
        
    def wait_for_api_reset(self, minutes=75):
        wait_sec = minutes * 60
        print(f"[FASTF1 BLOCK] Esperando {minutes} minutos antes de reintentar...")
        for i in range(minutes):
            print(f"Esperando... {minutes - i} min restantes")
            time.sleep(60)