import fastf1 as ff1
import os
import pandas as pd
from datetime import datetime
import time

# Habilitar caché para FastF1
cache_dir = 'cache'
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
ff1.Cache.enable_cache(cache_dir)

# Años objetivo
TARGET_YEARS = [2020, 2021, 2022, 2023]

# Función para guardar datos en CSV
def save_to_csv(data, folder_path, file_name):
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, file_name)
    if not os.path.exists(file_path):
        try:
            data.to_csv(file_path, index=False)
            print(f"Datos guardados en: {file_path}")
        except Exception as e:
            print(f"Error guardando el archivo {file_path}: {e}")
    else:
        print(f"Archivo ya existe, se omite: {file_path}")

# Función principal para extraer telemetría de clasificación para los tres primeros en carrera
for year in TARGET_YEARS:
    print(f"Procesando el año {year}...")
    try:
        schedule = ff1.get_event_schedule(year)

        for _, event in schedule.iterrows():
            circuit = event['EventName'].replace(" ", "_").replace("/", "-")
            print(f"Procesando evento: {circuit} ({year})")

            try:
                # Cargar sesión de carrera para identificar los tres primeros
                print(f"Cargando sesión: Carrera para {circuit} ({year})...")
                race_session = ff1.get_session(year, event['EventName'], "Race")
                race_session.load()
                print("Sesión de carrera cargada")

                # Obtener los tres primeros del podio
                race_results = race_session.results
                top_3 = race_results.iloc[:3]

                print("Cargando sesión: Clasificación para los tres primeros...")
                qualifying_session = ff1.get_session(year, event['EventName'], "Qualifying")
                qualifying_session.load()
                print("Sesión de clasificación cargada")

                for _, driver in top_3.iterrows():
                    driver_name = driver['FullName'].replace(" ", "_")
                    driver_number = driver['DriverNumber']
                    print(f"Procesando datos de: {driver_name} ({driver_number})")

                    # Crear carpeta específica para clasificación
                    base_folder = f"output/Qualifying/{year}/{circuit}/{driver_name}"
                    laps_file = os.path.join(base_folder, f"{driver_name}_laps.csv")
                    telemetry_file = os.path.join(base_folder, f"{driver_name}_telemetry.csv")
                    weather_file = os.path.join(base_folder, f"{driver_name}_weather.csv")

                    if os.path.exists(laps_file) and os.path.exists(telemetry_file) and os.path.exists(weather_file):
                        print(f"Todos los archivos para {driver_name} en {circuit} ({year}) ya existen. Se omite.")
                        continue

                    # Filtrar datos de clasificación para el piloto específico
                    laps = qualifying_session.laps.pick_driver(driver_number)
                    telemetry = laps.get_car_data().add_distance() if not laps.empty else pd.DataFrame()
                    weather = qualifying_session.weather_data

                    if not laps.empty:
                        save_to_csv(laps, base_folder, f"{driver_name}_laps.csv")
                    else:
                        print(f"No hay datos de vueltas para {driver_name} en {circuit} ({year}).")

                    if not telemetry.empty:
                        save_to_csv(telemetry, base_folder, f"{driver_name}_telemetry.csv")
                    else:
                        print(f"Telemetría vacía para {driver_name} en {circuit} ({year}).")

                    if not weather.empty:
                        save_to_csv(weather, base_folder, f"{driver_name}_weather.csv")
                    else:
                        print(f"Datos climáticos vacíos para {driver_name} en {circuit} ({year}).")

                    # Agregar un retraso para evitar saturar la API
                    time.sleep(5)

            except Exception as e:
                print(f"Error procesando el evento en {circuit} ({year}): {e}")

            # Agregar un retraso entre eventos para reducir la carga en la API
            time.sleep(10)

    except Exception as e:
        print(f"Error procesando el año {year}: {e}")

print("Extracción completada.")
