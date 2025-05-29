import os
import pandas as pd

# Directorio base donde están los datos
base_dir = "F:\\ESCOM-activo\\Trabajo terminal\\Extractor\\CombinedData"

# Carpetas de cada tipo de sesión
session_dirs = {
    "qualifying": "CombinedDataQualifying",
    "practice": "CombinedPracticeData",
    "race": "CombinedRaceData"
}

# Tipos de archivos a combinar
file_types = ["weather", "laps", "telemetry"]

# Carpeta donde se guardarán los archivos combinados
output_dir = os.path.join(base_dir, "F:\\ESCOM-activo\\Trabajo terminal\\Extractor\\CombinedDataResults")
os.makedirs(output_dir, exist_ok=True)

# Función para combinar archivos CSV por tipo y sesión
def combine_csvs(session_name, session_path, file_type):
    combined_df = pd.DataFrame()
    file_count = 0

    # Buscar archivos del tipo específico
    for root, _, files in os.walk(session_path):
        for file in files:
            if file.endswith(f"_{file_type}.csv"):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')  # Mantener compatibilidad con Modern_Spanish_CI_AS
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    file_count += 1
                except Exception as e:
                    print(f"Error al leer {file}: {e}")

    # Guardar archivo combinado
    if not combined_df.empty:
        output_file = os.path.join(output_dir, f"{session_name}_{file_type}_combined.csv")
        combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')  # Asegurar compatibilidad con collation
        print(f"Se combinaron {file_count} archivos en {output_file}")
    else:
        print(f"No se encontraron archivos de tipo '{file_type}' en la sesión '{session_name}'.")

# Ejecutar combinación para cada sesión y tipo de archivo
for session, session_folder in session_dirs.items():
    session_path = os.path.join(base_dir, session_folder)
    for file_type in file_types:
        combine_csvs(session, session_path, file_type)

print("Proceso finalizado. Todos los archivos combinados están en 'CombinedDataResults'.")
