import os
import pandas as pd

# Ruta base donde se encuentran los archivos CSV
base_path = r'F:\ESCOM-activo\Trabajo terminal\Extractor\CombinedDataResults'

def clean_and_convert_data(file_path):
    # Leer el archivo CSV
    df = pd.read_csv(file_path)
    
    # Añadir un número incremental al 'Identifier' para evitar duplicados
    if 'Identifier' in df.columns:
        # Añadir un número incremental único al 'Identifier' basado en su posición
        df['Identifier'] = df['Identifier'].astype(str) + df.groupby('Identifier').cumcount().astype(str)
    
    # Conversión para 'Rainfall' a VARCHAR ('True'/'False')
    if 'Rainfall' in df.columns:
        df['Rainfall'] = df['Rainfall'].replace({True: 'True', False: 'False'})
    
    # Conversión para 'IsPersonalBest' a VARCHAR ('True'/'False')
    if 'IsPersonalBest' in df.columns:
        df['IsPersonalBest'] = df['IsPersonalBest'].replace({True: 'True', False: 'False'})
    
    # Asegurarse de que 'WindSpeed' sea de tipo FLOAT (eliminar valores no numéricos si existen)
    if 'WindSpeed' in df.columns:
        df['WindSpeed'] = pd.to_numeric(df['WindSpeed'], errors='coerce')  # Convierta a NaN si no puede convertir
    
    # Asegurarse de que 'AirTemp', 'Humidity', 'Pressure', etc., sean de tipo FLOAT (en caso de que haya valores no numéricos)
    float_columns = ['AirTemp', 'Humidity', 'Pressure', 'TrackTemp', 'WindDirection', 'WindSpeed', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST', 'TyreLife']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convierta a NaN si no puede convertir
    
    # Convertir columnas de texto si es necesario
    text_columns = ['Circuito', 'Piloto', 'Gran_Premio', 'Tipo_Archivo', 'Driver', 'Team', 'Compound', 'FreshTyre']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Convertir valores nulos en 'NULL' solo para columnas de tipo texto
    for col in df.columns:
        if df[col].dtype == 'object':  # Asegurarse de que solo se cambien las columnas de texto
            df[col] = df[col].fillna('NULL')  # 'NULL' para columnas de texto
    
    # Guardar el archivo limpio en un nuevo archivo para importarlo a SQL Server
    cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
    df.to_csv(cleaned_file_path, index=False)
    
    print(f"Archivo limpio guardado en: {cleaned_file_path}")
    return cleaned_file_path

def process_all_files_in_directory(directory_path):
    # Obtener todos los archivos CSV en la carpeta especificada
    for file_name in os.listdir(directory_path):
        # Comprobar si el archivo tiene la extensión .csv
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory_path, file_name)
            print(f"Procesando el archivo: {file_path}")
            # Llamar a la función para limpiar y convertir los datos
            clean_and_convert_data(file_path)

# Ejecutar el procesamiento para todos los archivos en el directorio especificado
process_all_files_in_directory(base_path)
