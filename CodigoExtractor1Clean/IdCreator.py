import pandas as pd
import os

base_dir = "F:\\ESCOM-activo\\Trabajo terminal\\Extractor\\CombinedData\\CombinedDataResults"
output_dir = os.path.join(base_dir, "UpdatedFiles")
os.makedirs(output_dir, exist_ok=True)


files = [
    "qualifying_weather_combined.csv",
    "qualifying_laps_combined.csv",
    "qualifying_telemetry_combined.csv",
    "practice_weather_combined.csv",
    "practice_laps_combined.csv",
    "practice_telemetry_combined.csv",
    "race_weather_combined.csv",
    "race_laps_combined.csv",
    "race_telemetry_combined.csv"
]

# Función para extraer los IDs
def extract_info_from_identifier(identifier):
    
    parts = identifier.split('_')
    try:
        # Formato esperado: NombreGP_año_nombreCircuito_NombrePiloto_sesion_tipo
        gp_id = f"{parts[0]}_{parts[1]}_{parts[2]}"  # NombreGP_año_nombreCircuito
        circuit_name = " ".join(parts[3:-3])  # El nombre del circuito puede tener varias palabras
        driver_id = f"{parts[-3]}_{parts[-2]}"  # NombrePiloto_Apellido
        session_type = parts[-1].replace('.csv', '')  # Remover ".csv" del final
        return driver_id, gp_id, circuit_name, session_type
    except Exception as e:
        print(f"Error al procesar el identificador {identifier}: {e}")
        return None, None, None, None

# Función para agregar las columnas a cada archivo
def add_columns_to_dataframe(file_path):
    try:
        # Leer el archivo
        df = pd.read_csv(file_path)

        driver_id, gp_id, circuit_name, session_type = extract_info_from_identifier(df['Identifier'][0])
        
        if driver_id and gp_id and circuit_name:

            df['driver_id'] = driver_id
            df['gp_id'] = gp_id
            df['circuit_name'] = circuit_name
            
            
            output_file = os.path.join(output_dir, os.path.basename(file_path))
            df.to_csv(output_file, index=False)
            print(f"Archivo procesado y guardado: {output_file}")
        else:
            print(f"Error al procesar el archivo {file_path}. Los valores de ID no se pudieron extraer.")
    except Exception as e:
        print(f"Error al procesar el archivo {file_path}: {e}")

for file in files:
    file_path = os.path.join(base_dir, file)
    add_columns_to_dataframe(file_path)

print("Proceso finalizado. Archivos actualizados con los nuevos campos.")
