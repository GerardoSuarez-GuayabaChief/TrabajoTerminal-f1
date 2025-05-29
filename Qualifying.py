from DataExtractor import F1DataExtractor
import time
import fastf1 as ff1
import pandas as pd
import os
from requests.exceptions import HTTPError, ConnectionError

class QualifyingExtractor(F1DataExtractor):
    def extract(self, years, circuits=None, base_output_dir='output', callback=None):
        for year in years:
            schedule = self.get_schedule(year)
            if circuits:
                schedule = schedule[schedule['EventName'].isin(circuits)]

            for _, event in schedule.iterrows():
                circuit = event['EventName']
                retry = True
                while retry:
                    try:
                        print(f"Iniciando extracción de clasificación para {circuit} ({year})")
                        race_session = ff1.get_session(year, circuit, "Race")
                        race_session.load()
                        top_3 = race_session.results.iloc[:3]

                        qualifying_session = ff1.get_session(year, circuit, "Qualifying")
                        qualifying_session.load()

                        for _, driver in top_3.iterrows():
                            driver_name = driver['FullName'].replace(" ", "_")
                            number = driver['DriverNumber']

                            laps = qualifying_session.laps.pick_driver(number)
                            telemetry = laps.get_car_data().add_distance() if not laps.empty else pd.DataFrame()
                            weather = qualifying_session.weather_data

                            folder = os.path.join(base_output_dir, "Qualifying", str(year), circuit.replace(' ', '_'), driver_name)

                            self.save_to_csv(laps, folder, f"{driver_name}_laps.csv")
                            self.save_to_csv(telemetry, folder, f"{driver_name}_telemetry.csv")
                            self.save_to_csv(weather, folder, f"{driver_name}_weather.csv")
                            time.sleep(2)

                        if callback:
                            callback()
                        retry = False 

                    except (HTTPError, ConnectionError) as e:
                        print(f"Posible bloqueo de FastF1 en {circuit} ({year}): {e}")
                        self.wait_for_api_reset()
                        print("Reintentando...")

                    except Exception as e:
                        print(f"Error inesperado en clasificación de {circuit} ({year}): {e}")
                        retry = False

                time.sleep(3)