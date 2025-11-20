import json

def extract_matching_golden_queries(dataset_path, generated_path, output_path):
    # Leer dataset con golden queries
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    # Leer JSON con queries generadas por la LLM
    with open(generated_path, "r", encoding="utf-8") as f:
        generated = json.load(f)

    # Convertir lista → diccionario para búsqueda rápida por question_id
    golden_map = {item["question_id"]: item["SQL_golden"] for item in dataset}

    results = []

    for item in generated:
        qid = item["question_id"]

        if qid in golden_map:
            results.append({
                "question_id": qid,
                "SQL_golden": golden_map[qid]
            })
        else:
            print(f"⚠️ Warning: question_id {qid} not found in dataset.")

    # Guardar archivo final
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print(f"✅ Archivo generado correctamente en: {output_path}")
    print(f"Total de queries encontradas: {len(results)}")


if __name__ == "__main__":
    dataset_path = "datasets/db_questions.json"          # archivo completo con golden queries
    generated_path = "outputs/gens_SQL.json"             # archivo con SQLs generadas por la LLM
    output_path = "outputs/GoldenQueries.json"  # salida filtrada

    extract_matching_golden_queries(dataset_path, generated_path, output_path)
