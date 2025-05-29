import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
import os

from Qualifying import QualifyingExtractor
from Race import RaceExtractor
from Practice import PracticeExtractor
from Cleaner import clean_f1_data
from Combine import combine_all_sessions
from Enrich import enrich_identifiers
from Final_cleaner import clean_all
from SQL_bulk_loader import (
    connect_and_create_db,
    execute_structure_script,
    generate_bulk_commands_with_checks,
    execute_bulk_insert
)

# --- Interfaz principal ---
root = tk.Tk()
root.title("F1 Data Extractor, Transform & Loader")

# --- Entradas básicas ---
tk.Label(root, text="Años (ej. 2021,2022)").pack()
year_entry = tk.Entry(root)
year_entry.pack()

tk.Label(root, text="Circuitos (ej. Silverstone Circuit, Monza)").pack()
circuit_entry = tk.Entry(root, width=50)
circuit_entry.pack()

# --- Variables de selección de sesiones ---
qualifying_var = tk.BooleanVar()
practice_var = tk.BooleanVar()
race_var = tk.BooleanVar()

tk.Checkbutton(root, text="Extraer Qualifying", variable=qualifying_var).pack()
tk.Checkbutton(root, text="Extraer Practice", variable=practice_var).pack()
tk.Checkbutton(root, text="Extraer Race", variable=race_var).pack()

# --- Tipo de limpieza ---
cleaning_session = tk.StringVar(value="Practice")
tk.Label(root, text="Tipo de limpieza:").pack()
ttk.Combobox(root, textvariable=cleaning_session, values=["Practice", "Qualifying", "Race"]).pack()

# --- Campos para carga SQL Server ---
tk.Label(root, text="Servidor SQL Server (ej: localhost)").pack()
sql_server_entry = tk.Entry(root, width=40)
sql_server_entry.pack()

tk.Label(root, text="Ruta al script SQL de estructura:").pack()
sql_script_path = tk.Entry(root, width=60)
sql_script_path.pack()

tk.Label(root, text="Ruta a la carpeta con archivos combinados:").pack()
csv_path_entry = tk.Entry(root, width=60)
csv_path_entry.pack()

# --- Etiqueta de estado ---
status_label = tk.Label(root, text="Esperando instrucciones...")
status_label.pack()

# --- Visor de log ---
log_output = tk.Text(root, height=10, width=80)
log_output.pack(pady=5)

def log_message(msg):
    log_output.insert(tk.END, msg + "\n")
    log_output.see(tk.END)
    root.update()

# --- Funciones de control ---
def run_extraction():
    years = [int(y.strip()) for y in year_entry.get().split(',') if y.strip()]
    circuits = [c.strip() for c in circuit_entry.get().split(',') if c.strip()]
    output_dir = filedialog.askdirectory(title="Selecciona carpeta para guardar CSVs")

    if not (years and circuits and output_dir):
        messagebox.showwarning("Campos incompletos", "Completa los campos requeridos.")
        return

    try:
        if practice_var.get():
            log_message("Extrayendo Practice...")
            PracticeExtractor().extract(years, circuits, base_output_dir=output_dir, callback=lambda: log_message("Practice OK"))
        if qualifying_var.get():
            log_message("Extrayendo Qualifying...")
            QualifyingExtractor().extract(years, circuits, base_output_dir=output_dir, callback=lambda: log_message(" Qualifying OK"))
        if race_var.get():
            log_message("Extrayendo Race...")
            RaceExtractor().extract(years, circuits, base_output_dir=output_dir, callback=lambda: log_message("Race OK"))

        log_message("Ejecutando limpieza...")
        for session in ["Practice", "Qualifying", "Race"]:
            src = os.path.join(output_dir, session)
            dest = os.path.join(output_dir, "CombinedData", f"Combined{session}Data")
            clean_f1_data(session, src, dest, selected_circuits=circuits)

        log_message("Combinando CSVs...")
        combine_dir = os.path.join(output_dir, "CombinedData", "CombinedDataResults")
        combine_all_sessions(
            base_dir=os.path.join(output_dir, "CombinedData"),
            session_dirs={
                "qualifying": "CombinedQualifyingData",
                "practice": "CombinedPracticeData",
                "race": "CombinedRaceData"
            },
            file_types=["laps", "telemetry", "weather"],
            output_dir=combine_dir
        )

        log_message("Enriqueciendo identificadores...")
        enrich_identifiers(combine_dir, combine_dir)

        log_message("Limpieza final para SQL Server...")
        clean_all(combine_dir)

        messagebox.showinfo("Extracción completa", "Proceso completo exitoso.")

    except Exception as e:
        log_message(f"Error: {e}")
        messagebox.showerror("Error en extracción", str(e))

def run_cleaning():
    session_type = cleaning_session.get()
    source = filedialog.askdirectory(title="Selecciona carpeta de datos brutos")
    dest = filedialog.askdirectory(title="Selecciona carpeta para guardar limpios")
    circuits = circuit_entry.get().split(',')
    circuits = [c.strip() for c in circuits if c.strip()]
    if not (source and dest and circuits):
        messagebox.showwarning("Campos incompletos", "Completa los campos requeridos.")
        return

    try:
        log_message("Limpieza por sesión...")
        clean_f1_data(session_type, source, dest, selected_circuits=circuits)

        log_message("Combinando CSVs...")
        combine_dir = os.path.join(dest, "CombinedDataResults")
        combine_all_sessions(
            base_dir=dest,
            session_dirs={session_type.lower(): f"Combined{session_type}Data"},
            file_types=["laps", "telemetry", "weather"],
            output_dir=combine_dir
        )

        log_message("Enriqueciendo identificadores...")
        enrich_identifiers(combine_dir, combine_dir)

        log_message("Limpieza final para SQL Server...")
        clean_all(combine_dir)

        messagebox.showinfo("Limpieza completa", "Proceso completo exitoso.")

    except Exception as e:
        log_message(f"Error: {e}")
        messagebox.showerror("Error", str(e))

def load_to_sql_server():
    server = sql_server_entry.get().strip()
    db_name = "F1Telemetry"
    script_path = sql_script_path.get().strip()
    csv_dir = csv_path_entry.get().strip()

    if not (server and script_path and csv_dir):
        messagebox.showwarning("Campos vacíos", "Debes completar todos los campos.")
        return

    try:
        log_message("Verificando base de datos...")
        connect_and_create_db(server, db_name)

        log_message("Ejecutando script de estructura...")
        execute_structure_script(server, db_name, script_path)

        log_message("📥 Preparando comandos BULK INSERT...")
        bulk_script, skipped = generate_bulk_commands_with_checks(server, db_name, csv_dir, log_callback=log_message)

        if not bulk_script.strip():
            log_message("No hay comandos nuevos para ejecutar.")
            return

        log_message("Ejecutando carga BULK INSERT...")
        execute_bulk_insert(server, db_name, bulk_script)

        log_message("Carga completada.")
        if skipped:
            log_message("Archivos omitidos por datos ya existentes:")
            for f in skipped:
                log_message(f" {f}")

        messagebox.showinfo("Carga exitosa", "Los datos fueron cargados en SQL Server.")

    except Exception as e:
        log_message(f"Error: {e}")
        messagebox.showerror("Error", str(e))

# --- Botones ---
tk.Button(root, text="Ejecutar Extracción + Limpieza + Combinar", command=run_extraction).pack(pady=5)
tk.Button(root, text="Limpiar datos ya extraídos", command=run_cleaning).pack(pady=5)
tk.Button(root, text="Cargar en SQL Server", command=load_to_sql_server).pack(pady=10)

# --- Ejecutar interfaz ---
root.mainloop()