import json
from mysql.connector import connect, Error


# ----------------------------- EXECUTION -------------------------------- #

def execute_query(db_config, sql):
    """Ejecuta SQL y devuelve lista de tuplas; None si falla."""
    try:
        with connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return [tuple(r) for r in rows]
    except Error:
        return None


# -------------------------- METRICS ------------------------------------- #

def execution_accuracy(result_gold, result_gen):
    """Exact match: 1 si son idénticos, 0 caso contrario."""
    if result_gold is None or result_gen is None:
        return 0
    return 1 if result_gold == result_gen else 0


def partial_execution_accuracy(result_gold, result_gen):
    """
    P-Exec = |intersección| / |resultado_gold|
    “Qué porcentaje del resultado correcto logró devolver la query generada”.
    """
    if result_gold is None or not result_gold:
        return 1.0 if (result_gen is None or not result_gen) else 0.0
    if result_gen is None:
        return 0.0

    set_gold = set(result_gold)
    set_gen = set(result_gen)

    inter = len(set_gold & set_gen)
    return inter / len(set_gold)


def execution_result_distance(result_gold, result_gen):
    """
    ERD = 1 - Jaccard(resultados)
    Mide qué TAN diferentes son los resultados.
    """
    if result_gold is None or result_gen is None:
        return 1.0

    set_gold = set(result_gold)
    set_gen = set(result_gen)

    if not set_gold and not set_gen:
        return 0.0  # No hay diferencia

    inter = len(set_gold & set_gen)
    union = len(set_gold | set_gen)

    jaccard = inter / union if union > 0 else 0.0
    return 1 - jaccard


def evaluate_semantic(dataset_path, generated_path, output_path, db_config):
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    with open(generated_path, "r", encoding="utf-8") as f:
        generated = json.load(f)

    # Mapear golden queries
    golden_map = {item["question_id"]: item["SQL_golden"] for item in dataset}

    results = []

    for item in generated:
        qid = item["question_id"]
        sql_gen = item["SQL_generated"]
        sql_golden = golden_map.get(qid)

        if not sql_golden:
            print(f"⚠️ No golden SQL para question_id {qid}")
            continue

        # Ejecutar ambas queries
        gold_result = execute_query(db_config, sql_golden)
        gen_result = execute_query(db_config, sql_gen)

        # Métricas finales
        exe_acc = execution_accuracy(gold_result, gen_result)
        pexec = partial_execution_accuracy(gold_result, gen_result)
        erd = execution_result_distance(gold_result, gen_result)

        results.append({
            "question_id": qid,
            "result": {
                "execution_accuracy": exe_acc
                # "partial_execution_accuracy": round(pexec, 4),
                # "execution_result_distance": round(erd, 4)
            }
        })

        print(f"[QID {qid}] EXE={exe_acc} | PExec={pexec:.3f} | ERD={erd:.3f}")

    # Guardar salida
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print(f"\n✅ Semantic evaluation written to {output_path}")
    return results


# ------------------------------- MAIN ----------------------------------- #

if __name__ == "__main__":
    dataset_path = "outputs/GoldenQueries.json"
    generated_path = "outputs/gens_SQL.json"
    output_path = "outputs/semantic_evaluation.json"

    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "bird_db",
    }

    evaluate_semantic(dataset_path, generated_path, output_path, db_config)
