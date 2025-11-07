import json
import os


def get_schema_text(db_id, all_schemas):
    """
    Extrae el esquema textual de una base de datos específica a partir del JSON global de esquemas.
    """
    db_schema = next((db for db in all_schemas if db["db_id"] == db_id), None)
    if not db_schema:
        return f"-- Schema for database '{db_id}' not found.\n"

    schema_text = f"-- Database: {db_id}\n\n"
    for table in db_schema["table_names_original"]:
        table_index = db_schema["table_names_original"].index(table)
        table_cols = [
            col[1] for col in db_schema["column_names_original"]
            if col[0] == table_index
        ]
        table_types = [
            db_schema["column_types"][db_schema["column_names_original"].index(col)]
            for col in db_schema["column_names_original"]
            if col[0] == table_index
        ]
        schema_text += f"TABLE {table} (\n"
        for col_name, col_type in zip(table_cols, table_types):
            schema_text += f"  {col_name} {col_type},\n"
        schema_text = schema_text.rstrip(",\n") + "\n)\n\n"

    return schema_text


def generate_prompt(question, db_id, sql_dialect, all_schemas):
    """
    Genera un prompt completo para una pregunta específica.
    """
    schema_prompt = get_schema_text(db_id, all_schemas)

    base_prompt = f"-- Using valid {sql_dialect}, write an SQL query for the following request:\n"
    question_prompt = f"-- Question: {question}\n"
    cot_prompt = f"-- Think step by step before writing the query.\n"
    instruction_prompt = (
        "-- Return only the final SQL query.\n"
        "-- Do not include explanations, reasoning, or comments.\n"
        "-- Start your answer directly with the SELECT statement.\n"
    )

    return schema_prompt + "\n" + base_prompt + question_prompt + cot_prompt + instruction_prompt


def generate_prompts_from_dataset(dataset_path, schema_path, output_path, sql_dialect="MySQL", limit=None):
    """
    Genera un JSON con prompts a partir del dataset de preguntas y el archivo global de esquemas.
    Puede limitarse el número de prompts con el parámetro 'limit'.
    """
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    with open(schema_path, "r", encoding="utf-8") as f:
        all_schemas = json.load(f)

    # aplicar límite si se especifica
    if limit is not None:
        dataset = dataset[:limit]

    prompts = []
    for item in dataset:
        question_id = item["question_id"]
        db_id = item["db_id"]
        question = item["question"]

        prompt_text = generate_prompt(question, db_id, sql_dialect, all_schemas)
        prompts.append({
            "question_id": question_id,
            "db_id": db_id,
            "prompt": prompt_text
        })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(prompts, f, indent=4, ensure_ascii=False)

    print(f"✅ Prompts generated successfully in {output_path}")
    print(f"📊 Total prompts: {len(prompts)}")


if __name__ == "__main__":
    # Ejemplo de uso
    dataset_path = "datasets/db_questions.json"   # JSON con las preguntas
    schema_path = "datasets/tables.json"          # JSON global con los esquemas
    output_path = "outputs/prompts.json"          # salida con los prompts generados
    sql_dialect = "MySQL"

    # Puedes cambiar este valor para limitar cuántos prompts generar
    generate_prompts_from_dataset(dataset_path, schema_path, output_path, sql_dialect, limit=50)
