import os
import pandas as pd

# Mapeo de GP a circuito
gp_to_circuit = {
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

# Lista de circuitos a incluir
selected_circuits = {
    "Silverstone Circuit",
    "Suzuka International Racing Course",
    "Circuit de Monaco",
    "Autodromo Nazionale di Monza",
    "Circuit de Spa-Francorchamps"
}

# Ruta de origen y destino
source_folder = "F:/ESCOM-activo/Trabajo terminal/Extractor/output"
destination_folder = "F:/ESCOM-activo/Trabajo terminal/Extractor/output/CombinedData/CombinedPracticeData"
os.makedirs(destination_folder, exist_ok=True)

# Recorrer estructura de carpetas
for year in os.listdir(source_folder):
    year_path = os.path.join(source_folder, year)
    if not os.path.isdir(year_path):
        continue
    
    for grand_prix in os.listdir(year_path):
        grand_prix_path = os.path.join(year_path, grand_prix)
        if not os.path.isdir(grand_prix_path):
            continue
        
        circuit_name = gp_to_circuit.get(grand_prix, "Unknown_Circuit")
        if circuit_name not in selected_circuits:
            continue
        
        for driver in os.listdir(grand_prix_path):
            driver_path = os.path.join(grand_prix_path, driver)
            if not os.path.isdir(driver_path):
                continue
            
            for session in ["FP1", "FP2", "FP3"]:
                session_path = os.path.join(driver_path, session)
                if not os.path.isdir(session_path):
                    continue
                
                for file in os.listdir(session_path):
                    if file.endswith(".csv"):
                        file_type = file.split('_')[-1].replace('.csv', '')
                        identifier = f"{grand_prix}_{year}_{circuit_name}_{driver}_{session}_FreePractice_{file_type}"
                        new_filename = f"{identifier}.csv"
                        new_filepath = os.path.join(destination_folder, new_filename)
                        
                        df = pd.read_csv(os.path.join(session_path, file))
                        
                        # Agregar columnas requeridas
                        df.insert(0, 'Circuito', circuit_name)
                        df.insert(1, 'Piloto', driver)
                        df.insert(2, 'Gran_Premio', grand_prix)
                        df.insert(3, 'Año', year)
                        df.insert(4, 'Tipo_Archivo', file_type)
                        df.insert(5, 'Identifier', identifier)
                        
                        # Guardar archivo con la collation Modern_Spanish_CI_AS asegurando UTF-8 encoding
                        df.to_csv(new_filepath, index=False, encoding='utf-8-sig')
                        
                        print(f"Processed: {new_filename}")

print("Free practice data processing complete.")