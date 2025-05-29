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
    # Renombrar columna Año si aparece con caracteres especiales
    df.rename(columns={df.columns[3]: 'Año'}, inplace=True)
    
    # Limpiar columnas numéricas (float)
    for column in df.select_dtypes(include=['float64']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].apply(lambda x: round(float(x), 6) if pd.notna(x) else None)
    
    # Limpiar columnas enteras (int)
    for column in df.select_dtypes(include=['int64']).columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].apply(lambda x: int(x) if pd.notna(x) else None)
    
    # Convertir valores booleanos a 1/0 (para Brake)
    if 'Brake' in df.columns:
        df['Brake'] = df['Brake'].astype(int)
    
    # Convertir valores de tiempo en formato '0 days HH:MM:SS.ssssss' a HH:MM:SS.sss
    for column in ['Time', 'SessionTime']:
        if column in df.columns:
            df[column] = df[column].astype(str).apply(lambda x: x.split()[-1] if ' days ' in x else x)
            df[column] = pd.to_datetime(df[column], format='%H:%M:%S.%f', errors='coerce')
            df[column] = df[column].apply(lambda x: x.strftime('%H:%M:%S.%f')[:-3] if pd.notna(x) else None)
    
    # Convertir fechas en formato YYYY-MM-DD HH:MM:SS a STRING para SQL Server
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
    
    # Convertir NaN a None en todas las columnas
    df = df.replace({np.nan: None})
    
    return df

# Función para insertar datos en la tabla Telemetry
def insert_data_to_telemetry(file_path):
    df = pd.read_csv(file_path)
    df = clean_data(df)
    
    data_to_insert = []
    for _, row in df.iterrows():
        identifier = row['Identifier']
        
        if check_if_exists(identifier, 'Telemetry'):
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
        sql = f"INSERT INTO Telemetry ({', '.join(df.columns)}) VALUES ({placeholders})"
        try:
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"{len(data_to_insert)} registros insertados en Telemetry.")
        except pyodbc.ProgrammingError as e:
            print("Error al insertar datos:", e)
            print("Valores de la primera fila que falló:")
            print(data_to_insert[0])
        except Exception as ex:
            print("Error inesperado:", ex)
            print("Revisando datos antes de la inserción:")
            for i, val in enumerate(data_to_insert[0]):
                print(f"Parámetro {i + 1}: {val} ({type(val)})")
    else:
        print("No se insertaron registros nuevos.")

# Ruta de la carpeta que contiene los archivos de Telemetry
base_path = r'F:\ESCOM-activo\Trabajo terminal\Extractor\CombinedDataResults\Clean'

# Lista de archivos de Telemetry a procesar
telemetry_files = [
    'practice_telemetry_combined_cleaned_cleaned.csv',
    'qualifying_telemetry_combined_cleaned_cleaned.csv',
    'race_telemetry_combined_cleaned_cleaned.csv'
]

# Procesar cada archivo en la carpeta
for file_name in telemetry_files:
    file_path = os.path.join(base_path, file_name)
    if os.path.exists(file_path):
        print(f"Procesando archivo: {file_path}")
        insert_data_to_telemetry(file_path)
    else:
        print(f"Archivo no encontrado: {file_path}")

# Cerrar la conexión
cursor.close()
conn.close()
