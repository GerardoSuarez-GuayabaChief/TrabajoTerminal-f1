import os
import pyodbc

def connect_and_create_db(server, db_name):
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;Trusted_Connection=yes;', autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]")
    cursor.close()
    conn.close()
    print(f"Base de datos '{db_name}' verificada o creada.")

def execute_structure_script(server, db_name, script_path):
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;', autocommit=True)
    cursor = conn.cursor()
    with open(script_path, 'r', encoding='utf-8') as f:
        script = f.read().split('GO')
        for cmd in script:
            cmd = cmd.strip()
            if cmd:
                try:
                    cursor.execute(cmd)
                except Exception as e:
                    print(f"Error ejecutando comando: {e}")
    conn.close()
    print("Script de estructura ejecutado correctamente.")

def generate_bulk_commands_with_checks(server, db_name, csv_dir, log_callback=None):
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;',
        autocommit=True
    )
    cursor = conn.cursor()

    table_map = {"telemetry": "Telemetry", "laps": "Laps", "weather": "Weather"}
    sessions = ["qualifying", "practice", "race"]
    commands = [f"USE {db_name}"]
    files_skipped = []

    for session in sessions:
        for key, table in table_map.items():
            filename = f"{session}_{key}_combined.csv"
            fullpath = os.path.join(csv_dir, filename)
            prefix = f"{session}_{key}"

            if os.path.exists(fullpath):
                if table_contains_prefix(cursor, table, prefix):
                    if log_callback:
                        log_callback(f"Datos ya cargados para {prefix}. Se omiten.")
                    files_skipped.append(filename)
                    continue
                commands.append(f"""
                    BULK INSERT {table}
                    FROM '{fullpath}'
                    WITH (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        FIRSTROW = 2
                    );
                    """)
                if log_callback:
                    log_callback(f"Preparado BULK INSERT para: {filename}")
            else:
                if log_callback:
                    log_callback(f"Archivo no encontrado: {filename}")

    conn.close()
    return "\n".join(commands), files_skipped

def generate_bulk_insert_commands(csv_dir, db_name):
    table_map = {"telemetry": "Telemetry", "laps": "Laps", "weather": "Weather"}
    sessions = ["qualifying", "practice", "race"]
    commands = [f"USE {db_name}"]
    for session in sessions:
        for key, table in table_map.items():
            filename = f"{session}_{key}_combined.csv"
            fullpath = os.path.join(csv_dir, filename)
            if os.path.exists(fullpath):
                commands.append(f"""
                    BULK INSERT {table}
                    FROM '{fullpath}'
                    WITH (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        FIRSTROW = 2
                    );
                    """)
    return "\n".join(commands)

def table_contains_prefix(cursor, table, prefix):
    try:
        query = f"SELECT COUNT(*) FROM {table} WHERE Identifier LIKE ?"
        cursor.execute(query, f"{prefix}%")
        count = cursor.fetchone()[0]
        return count > 0
    except:
        return False

def generate_bulk_commands_with_checks(server, db_name, csv_dir, log_callback=None):
    import pyodbc
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;', autocommit=True)
    cursor = conn.cursor()

    table_map = {"telemetry": "Telemetry", "laps": "Laps", "weather": "Weather"}
    sessions = ["qualifying", "practice", "race"]
    commands = [f"USE {db_name}"]
    files_skipped = []

    for session in sessions:
        for key, table in table_map.items():
            filename = f"{session}_{key}_combined.csv"
            fullpath = os.path.join(csv_dir, filename)
            prefix = f"{session}_{key}"

            if os.path.exists(fullpath):
                if table_contains_prefix(cursor, table, prefix):
                    if log_callback:
                        log_callback(f"Datos ya cargados para {prefix}. Se omiten.")
                    files_skipped.append(filename)
                    continue
                commands.append(f"""
                BULK INSERT {table}
                FROM '{fullpath}'
                WITH (
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\\n',
                    FIRSTROW = 2
                );
                """)
                if log_callback:
                    log_callback(f"Preparado BULK INSERT para: {filename}")
            else:
                if log_callback:
                    log_callback(f"Archivo no encontrado: {filename}")

    conn.close()
    return "\n".join(commands), files_skipped

def execute_bulk_insert(server, db_name, sql_script):
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;', autocommit=True)
    cursor = conn.cursor()
    commands = sql_script.split(";")
    for cmd in commands:
        if cmd.strip():
            try:
                cursor.execute(cmd)
            except Exception as e:
                print(f"Error en BULK INSERT: {e}")
    conn.close()
    print("BULK INSERT completado.")