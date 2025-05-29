import os
import pandas as pd

def extract_info_from_identifier(identifier):
    parts = identifier.split('_')
    try:
        gp_id = f"{parts[0]}_{parts[1]}_{parts[2]}"
        circuit_name = " ".join(parts[3:-3])
        driver_id = f"{parts[-3]}_{parts[-2]}"
        session_type = parts[-1].replace('.csv', '')
        return driver_id, gp_id, circuit_name, session_type
    except:
        return None, None, None, None

def enrich_identifiers(base_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for file in os.listdir(base_dir):
        if file.endswith(".csv") and not file.endswith("_cleaned.csv"):
            try:
                df = pd.read_csv(os.path.join(base_dir, file))
                driver_id, gp_id, circuit_name, _ = extract_info_from_identifier(df['Identifier'][0])
                df['driver_id'] = driver_id
                df['gp_id'] = gp_id
                df['circuit_name'] = circuit_name
                out_file = os.path.join(output_dir, file)
                df.to_csv(out_file, index=False, encoding='utf-8-sig')
                print(f"Enriquecido: {out_file}")
            except Exception as e:
                print(f"Error al procesar {file}: {e}")