import pandas as pd
import pyodbc
import os
import numpy as np

# Configurar conexión a SQL Server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=localhost;'
                      'DATABASE=F1Telemetry;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# Función para verificar si un identificador ya existe en la base de datos
def check_if_exists(identifier, table_name):
    query = f"SELECT 1 FROM {table_name} WHERE Identifier = ?"
    cursor.execute(query, (identifier,))
    return cursor.fetchone() is not None

# Función para limpiar los datos
def clean_data(df):
    # Limpiar columnas numéricas (float)
    for column in df.select_dtypes(include=['float64']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')  # Convertir valores inválidos a NaN
        df[column] = df[column].apply(lambda x: round(float(x), 6) if pd.notna(x) else None)  # Redondeo seguro
    
    # Limpiar columnas enteras (int)
    for column in df.select_dtypes(include=['int64']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].apply(lambda x: int(x) if pd.notna(x) else None)
    
    # Convertir valores booleanos a 1/0
    for column in df.select_dtypes(include=['bool']).columns:
        df[column] = df[column].astype(int)
    
    # Convertir valores de texto 'False' y 'True' a 0 y 1 si la columna es de tipo object
    for column in df.select_dtypes(include=['object']).columns:
        if df[column].dropna().astype(str).isin(['false', 'true']).any():
            df[column] = df[column].str.lower().map({'true': 1, 'false': 0})
    
    # Convertir valores de tiempo en formato '0 days HH:MM:SS.ssssss' a HH:MM:SS.sss
    for column in df.select_dtypes(include=['object']).columns:
        if df[column].astype(str).str.contains(' days ', na=False).any():
            df[column] = df[column].apply(lambda x: x.split()[-1] if isinstance(x, str) else None)
    
    # Convertir valores de tiempo en formato HH:MM:SS.ssssss a HH:MM:SS.sss
    for column in df.select_dtypes(include=['object']).columns:
        if df[column].astype(str).str.match(r'\d{2}:\d{2}:\d{2}\.\d{6}').any():
            df[column] = pd.to_datetime(df[column], format='%H:%M:%S.%f', errors='coerce')
            df[column] = df[column].apply(lambda x: x.strftime('%H:%M:%S.%f')[:-3] if pd.notna(x) else None)
    
    # Convertir fechas en formato YYYY-MM-DD HH:MM:SS a STRING para SQL Server
    for column in df.select_dtypes(include=['datetime64']).columns:
        df[column] = df[column].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
    
    # Convertir NaN a None en todas las columnas
    df = df.replace({np.nan: None})
    
    return df

# Función para insertar datos en la tabla Laps
def insert_data_to_laps(file_path):
    df = pd.read_csv(file_path)
    df = clean_data(df)
    
    data_to_insert = []
    for _, row in df.iterrows():
        identifier = row['Identifier']
        
        if check_if_exists(identifier, 'Laps'):
            print(f"El identificador {identifier} ya existe. Saltando la fila.")
            continue
        
        try:
            row_tuple = tuple(row)
            data_to_insert.append(row_tuple)
        except Exception as e:
            print(f"Error al procesar fila con Identifier {identifier}: {e}")
            print(row)
    
    if data_to_insert:
        placeholders = ', '.join(['?'] * len(df.columns))
        sql = f"INSERT INTO Laps ({', '.join(df.columns)}) VALUES ({placeholders})"
        try:
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"{len(data_to_insert)} registros insertados en Laps.")
        except pyodbc.ProgrammingError as e:
            print("Error al insertar datos:", e)
            print("Valores de la primera fila que falló:")
            print(data_to_insert[0])  # Muestra la primera fila con error
        except Exception as ex:
            print("Error inesperado:", ex)
            print("Revisando datos antes de la inserción:")
            for i, val in enumerate(data_to_insert[0]):
                print(f"Parámetro {i + 1}: {val} ({type(val)})")
    else:
        print("No se insertaron registros nuevos.")

# Ruta de la carpeta que contiene los archivos de Laps
base_path = r'F:\ESCOM-activo\Trabajo terminal\Extractor\CombinedDataResults\Clean'

# Lista de archivos de Laps a procesar
laps_files = [
    'practice_laps_combined_cleaned_cleaned.csv',
    'qualifying_laps_combined_cleaned_cleaned.csv',
    'race_laps_combined_cleaned_cleaned.csv'
]

# Procesar cada archivo en la carpeta
for file_name in laps_files:
    file_path = os.path.join(base_path, file_name)
    if os.path.exists(file_path):
        print(f"Procesando archivo: {file_path}")
        insert_data_to_laps(file_path)
    else:
        print(f"Archivo no encontrado: {file_path}")

# Cerrar la conexión
cursor.close()
conn.close()
