import json
import re
from Levenshtein import distance as levenshtein_distance
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def normalize_sql(sql: str) -> str:
    """
    Limpia y normaliza una consulta SQL para comparaciones más justas.
    """
    sql = sql.lower()
    sql = re.sub(r'`', '', sql)
    sql = re.sub(r'\s+', ' ', sql)
    sql = re.sub(r' as ', ' ', sql)
    sql = re.sub(r'([a-z0-9_]+)\s*\.', '', sql) 
    sql = re.sub(r';', '', sql)
    sql = sql.strip()
    return sql


def extract_attributes(sql: str):
    """
    Extrae los posibles nombres de atributos (columnas) de la query.
    """
    tokens = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql.lower())
    reserved = {
        "select", "from", "where", "and", "or", "join", "on", "as", "sum",
        "count", "avg", "min", "max", "group", "by", "order", "limit",
        "inner", "left", "right", "outer", "cast", "case", "when", "then",
        "else", "end", "distinct", "having", "like", "in", "between",
        "union", "all"
    }
    return [t for t in tokens if t not in reserved]


def levenshtein_similarity(a: str, b: str) -> float:
    """
    Similaridad de Levenshtein normalizada.
    """
    if not a or not b:
        return 0.0
    dist = levenshtein_distance(a, b)
    return 1 - dist / max(len(a), len(b))


def cosine_similarity_text(a: str, b: str) -> float:
    """
    Similaridad de coseno entre dos strings usando TF-IDF.
    """
    vectorizer = TfidfVectorizer(token_pattern=r'\b[a-zA-Z_][a-zA-Z0-9_]*\b')
    tfidf = vectorizer.fit_transform([a, b])
    return float(cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0])


def lexical_accuracy(golden_sql: str, generated_sql: str) -> float:
    """
    Porcentaje de atributos coincidentes entre la query generada y la referencia.
    """
    attrs_gold = set(extract_attributes(golden_sql))
    attrs_gen = set(extract_attributes(generated_sql))
    if not attrs_gold:
        return 0.0
    return len(attrs_gold & attrs_gen) / len(attrs_gold)


def evaluate_lexical_metrics(dataset_path, generated_path, alpha=0.3, beta=0.4, gamma=0.3):
    """
    Evalúa las métricas léxicas (Levenshtein, Cosine y Lexical Accuracy)
    entre las SQL golden y generadas.
    """
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)
    with open(generated_path, "r", encoding="utf-8") as f:
        generated = json.load(f)

    # Mapear question_id -> golden SQL
    golden_map = {item["question_id"]: item["SQL"] for item in dataset}

    results = []
    for item in generated:
        qid = item["question_id"]
        gen_sql = item["SQL_generated"]
        gold_sql = golden_map.get(qid)

        if not gold_sql:
            continue

        gen_sql_norm = normalize_sql(gen_sql)
        gold_sql_norm = normalize_sql(gold_sql)

        lev_sim = levenshtein_similarity(gold_sql_norm, gen_sql_norm)
        cos_sim = cosine_similarity_text(gold_sql_norm, gen_sql_norm)
        lex_acc = lexical_accuracy(gold_sql_norm, gen_sql_norm)

        lex_score = alpha * lev_sim + beta * cos_sim + gamma * lex_acc

        results.append({
            "question_id": qid,
            "levenshtein_similarity": round(lev_sim, 4),
            "cosine_similarity": round(cos_sim, 4),
            "lexical_accuracy": round(lex_acc, 4),
            "lexical_score": round(lex_score, 4)
        })

        print(f"[QID {qid}] Levenshtein={lev_sim:.3f} | Cosine={cos_sim:.3f} | LexAcc={lex_acc:.3f} → LexScore={lex_score:.3f}")

    avg_score = sum(r["lexical_score"] for r in results) / len(results)
    print(f"\n✅ Average Lexical Score: {avg_score:.4f}")

    with open("outputs/lexical_evaluation.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    return results


if __name__ == "__main__":
    dataset_path = "datasets/db_questions.json"   # golden queries
    generated_path = "outputs/gens_SQL.json" # queries de Claude
    evaluate_lexical_metrics(dataset_path, generated_path)
