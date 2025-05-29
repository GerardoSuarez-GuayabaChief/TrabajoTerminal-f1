import os
import pandas as pd

def clean_and_convert_data(file_path):
    df = pd.read_csv(file_path)
    
    if 'Identifier' in df.columns:
        df['Identifier'] = df['Identifier'].astype(str) + df.groupby('Identifier').cumcount().astype(str)

    if 'Rainfall' in df.columns:
        df['Rainfall'] = df['Rainfall'].replace({True: 'True', False: 'False'})
    if 'IsPersonalBest' in df.columns:
        df['IsPersonalBest'] = df['IsPersonalBest'].replace({True: 'True', False: 'False'})

    float_cols = ['AirTemp', 'Humidity', 'Pressure', 'TrackTemp', 'WindDirection', 'WindSpeed',
                  'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST', 'TyreLife']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    text_cols = ['Circuito', 'Piloto', 'Gran_Premio', 'Tipo_Archivo', 'Driver', 'Team', 'Compound', 'FreshTyre']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('NULL')

    output_file = file_path.replace('.csv', '_cleaned.csv')
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Archivo limpio final: {output_file}")

def clean_all(directory):
    for file in os.listdir(directory):
        if file.endswith(".csv") and not file.endswith("_cleaned.csv"):
            clean_and_convert_data(os.path.join(directory, file))