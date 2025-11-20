import mysql.connector

def generate_schema_prompt(sql_dialect, db_config_path):
    import json
    with open(db_config_path) as f:
        cfg = json.load(f)

    conn = mysql.connector.connect(**cfg)
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES;")
    tables = [t[0] for t in cursor.fetchall()]
    schema_info = ""

    for table in tables:
        cursor.execute(f"DESCRIBE `{table}`")
        columns = cursor.fetchall()
        schema_info += f"\n-- Table {table}:\n"
        for col in columns:
            schema_info += f"-- {col[0]} ({col[1]})\n"

    conn.close()
    return f"-- Database Schema ({sql_dialect}):\n{schema_info}"
