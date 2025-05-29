import os
import pandas as pd

GP_TO_CIRCUIT = {
    "Austrian_Grand_Prix": "Red Bull Ring",
    "Styrian_Grand_Prix": "Red Bull Ring",
    "Hungarian_Grand_Prix": "Hungaroring",
    "British_Grand_Prix": "Silverstone Circuit",
    "70th_Anniversary_Grand_Prix": "Silverstone Circuit",
    "Spanish_Grand_Prix": "Circuit de Barcelona-Catalunya",
    "Belgian_Grand_Prix": "Circuit de Spa-Francorchamps",
    "Italian_Grand_Prix": "Autodromo Nazionale di Monza",
    "Tuscan_Grand_Prix": "Autodromo Internazionale del Mugello",
    "Russian_Grand_Prix": "Sochi Autodrom",
    "Eifel_Grand_Prix": "Nürburgring",
    "Portuguese_Grand_Prix": "Autódromo Internacional do Algarve",
    "Emilia_Romagna_Grand_Prix": "Autodromo Internazionale Enzo e Dino Ferrari",
    "Turkish_Grand_Prix": "Istanbul Park",
    "Bahrain_Grand_Prix": "Bahrain International Circuit",
    "Sakhir_Grand_Prix": "Bahrain International Circuit (Outer Circuit)",
    "Abu_Dhabi_Grand_Prix": "Yas Marina Circuit",
    "Monaco_Grand_Prix": "Circuit de Monaco",
    "Azerbaijan_Grand_Prix": "Baku City Circuit",
    "French_Grand_Prix": "Circuit Paul Ricard",
    "Dutch_Grand_Prix": "Circuit Zandvoort",
    "United_States_Grand_Prix": "Circuit of the Americas",
    "Mexico_City_Grand_Prix": "Autódromo Hermanos Rodríguez",
    "São_Paulo_Grand_Prix": "Interlagos Circuit",
    "Qatar_Grand_Prix": "Losail International Circuit",
    "Saudi_Arabian_Grand_Prix": "Jeddah Corniche Circuit",
    "Australian_Grand_Prix": "Albert Park Circuit",
    "Miami_Grand_Prix": "Miami International Autodrome",
    "Canadian_Grand_Prix": "Circuit Gilles Villeneuve",
    "Singapore_Grand_Prix": "Marina Bay Street Circuit",
    "Japanese_Grand_Prix": "Suzuka International Racing Course",
    "Las_Vegas_Grand_Prix": "Las Vegas Strip Circuit"
}

def clean_f1_data(session_type: str, source_dir: str, output_dir: str, selected_circuits: list[str]):

    print(f"🔍 Procesando limpieza para sesión: {session_type}")
    os.makedirs(output_dir, exist_ok=True)

    for year in os.listdir(source_dir):
        year_path = os.path.join(source_dir, year)
        if not os.path.isdir(year_path):
            continue

        for grand_prix in os.listdir(year_path):
            gp_path = os.path.join(year_path, grand_prix)
            if not os.path.isdir(gp_path):
                continue

            circuit_name = GP_TO_CIRCUIT.get(grand_prix, "Unknown_Circuit")
            if circuit_name not in selected_circuits:
                continue  # ← Ahora usa la lista dinámica

            for driver in os.listdir(gp_path):
                driver_path = os.path.join(gp_path, driver)
                if not os.path.isdir(driver_path):
                    continue

                if session_type == "Practice":
                    for session in ["FP1", "FP2", "FP3"]:
                        session_path = os.path.join(driver_path, session)
                        if not os.path.isdir(session_path):
                            continue

                        for file in os.listdir(session_path):
                            if file.endswith(".csv"):
                                save_cleaned_csv(file, session_path, grand_prix, year, circuit_name, driver, f"{session}_FreePractice", output_dir)
                else:
                    if session_type == "Race" and driver in {"FP1", "FP2", "FP3", "Qualifying"}:
                        continue

                    for file in os.listdir(driver_path):
                        if file.endswith(".csv"):
                            save_cleaned_csv(file, driver_path, grand_prix, year, circuit_name, driver, session_type, output_dir)

    print(f"Limpieza completada para {session_type}")

def save_cleaned_csv(file, path, gp, year, circuit, driver, session_label, output_dir):
    file_type = file.split('_')[-1].replace('.csv', '')
    identifier = f"{gp}_{year}_{circuit}_{driver}_{session_label}_{file_type}"
    new_filename = f"{identifier}.csv"
    new_path = os.path.join(output_dir, new_filename)

    try:
        df = pd.read_csv(os.path.join(path, file))
        df.insert(0, 'Circuito', circuit)
        df.insert(1, 'Piloto', driver)
        df.insert(2, 'Gran_Premio', gp)
        df.insert(3, 'Año', year)
        df.insert(4, 'Tipo_Archivo', file_type)
        df.insert(5, 'Identifier', identifier)
        df.to_csv(new_path, index=False, encoding='utf-8-sig')
        print(f"Procesado: {new_filename}")
    except Exception as e:
        print(f"Error procesando {file}: {e}")