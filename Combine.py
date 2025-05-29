import os
import pandas as pd

def combine_csvs(session_name, session_path, file_type, output_dir):
    combined_df = pd.DataFrame()
    file_count = 0

    for root, _, files in os.walk(session_path):
        for file in files:
            if file.endswith(f"_{file_type}.csv"):
                try:
                    df = pd.read_csv(os.path.join(root, file), encoding='utf-8-sig')
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                    file_count += 1
                except Exception as e:
                    print(f"Error al leer {file}: {e}")

    if not combined_df.empty:
        out_file = os.path.join(output_dir, f"{session_name}_{file_type}_combined.csv")
        combined_df.to_csv(out_file, index=False, encoding='utf-8-sig')
        print(f"Combinado ({file_count} archivos): {out_file}")
    else:
        print(f"No se encontraron archivos {file_type} en {session_name}.")

def combine_all_sessions(base_dir, session_dirs, file_types, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for session, folder in session_dirs.items():
        full_path = os.path.join(base_dir, folder)
        for file_type in file_types:
            combine_csvs(session, full_path, file_type, output_dir)