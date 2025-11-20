import json
import re
import sqlparse
from difflib import SequenceMatcher
import networkx as nx



def normalize_sql(sql: str) -> str:
    """
    Limpia y normaliza una consulta SQL para comparaciones más justas.
    """
    sql = sql.lower()
    sql = re.sub(r'`', '', sql)
    sql = re.sub(r'\s+', ' ', sql)
    sql = re.sub(r' as ', ' ', sql)
    sql = re.sub(r';', '', sql)
    sql = sql.strip()
    return sql

def extract_tables(sql: str):
    """Extrae los nombres de tablas y sus alias de una consulta."""
    sql = sql.lower()
    tables = re.findall(r'(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+as\s+([a-zA-Z_][a-zA-Z0-9_]*))?', sql)
    table_map = {}
    for t in tables:
        table_name, alias = t
        table_map[alias if alias else table_name] = table_name
    return table_map


def extract_joins(sql: str):
    """Extrae todas las condiciones JOIN ... ON ... y devuelve pares de alias o tablas."""
    sql = sql.lower()
    # Captura todos los bloques JOIN ... ON ... hasta WHERE o siguiente JOIN
    join_blocks = re.findall(r'join\s+[a-zA-Z_][a-zA-Z0-9_]*.*?on\s+([^;]+?)(?:\s+join|\s+where|$)', sql)
    joins = []
    for jb in join_blocks:
        # Busca expresiones tipo a.col = b.col
        pairs = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\.[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)\.[a-zA-Z_][a-zA-Z0-9_]*', jb)
        joins.extend(pairs)
    return joins


def build_graph(sql: str):
    """Construye un grafo de relaciones entre tablas a partir de los JOINs."""
    table_map = extract_tables(sql)
    G = nx.Graph()

    # Añadimos los nodos base (todas las tablas)
    for real_table in table_map.values():
        G.add_node(real_table)

    joins = extract_joins(sql)
    for a, b in joins:
        real_a = table_map.get(a, a)
        real_b = table_map.get(b, b)
        if real_a != real_b:
            G.add_edge(real_a, real_b)

    return G



def tree_matching_similarity(sql1: str, sql2: str) -> float:
    """
    Similitud de estructura aproximada basada en tokens.
    """
    parsed1 = sqlparse.parse(sql1)
    parsed2 = sqlparse.parse(sql2)
    tokens1 = [t.value.lower() for t in parsed1[0].tokens if not t.is_whitespace]
    tokens2 = [t.value.lower() for t in parsed2[0].tokens if not t.is_whitespace]
    ratio = SequenceMatcher(None, tokens1, tokens2).ratio()
    return round(ratio, 4)


def graph_similarity(sql1: str, sql2: str) -> float:
    """
    Similitud de grafos construidos a partir de los JOINs.
    """
    G1, G2 = build_graph(sql1), build_graph(sql2)
    E1, E2 = set(G1.edges()), set(G2.edges())
    if not E1 and not E2:
        return 1.0
    if not (E1 or E2):
        return 0.0

    intersection = len(E1 & E2)
    union = len(E1 | E2)
    return round(intersection / union, 4)


def join_correctness(sql1: str, sql2: str) -> float:
    """
    Evalúa si las uniones (JOIN ... ON ...) coinciden entre ambas queries.
    """
    joins1 = extract_joins(sql1)
    joins2 = extract_joins(sql2)
    if not joins1:
        return 1.0 if not joins2 else 0.0
    matches = sum(1 for j in joins1 if any(j2 in j or j in j2 for j2 in joins2))
    return round(matches / len(joins1), 4)



def evaluate_structural_metrics(dataset_path, generated_path, alpha=0.7, beta=0.15, gamma=0.15):
    """
    Evalúa métricas estructurales (Tree Matching, Graph Similarity y Join Correctness)
    entre las SQL golden y generadas.
    """
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)
    with open(generated_path, "r", encoding="utf-8") as f:
        generated = json.load(f)

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
        tree_sim = tree_matching_similarity(gold_sql_norm, gen_sql_norm)
        graph_sim = graph_similarity(gold_sql_norm, gen_sql_norm)
        join_acc = join_correctness(gold_sql_norm, gen_sql_norm)

        struct_score = round(alpha * tree_sim + beta * graph_sim + gamma * join_acc, 4)

        results.append({
            "question_id": qid,
            "tree_similarity": tree_sim,
            "graph_similarity": graph_sim,
            "join_correctness": join_acc,
            "structural_score": struct_score
        })

        print(f"[QID {qid}] TreeSim={tree_sim:.3f} | GraphSim={graph_sim:.3f} | JoinAcc={join_acc:.3f} → StructScore={struct_score:.3f}")

    avg_score = sum(r["structural_score"] for r in results) / len(results)
    print(f"\n✅ Average Structural Score: {avg_score:.4f}")

    with open("outputs/structural_evaluation.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    return results


if __name__ == "__main__":
    dataset_path = "datasets/db_questions.json"     # golden queries
    generated_path = "outputs/gens_SQL.json"        # queries generadas por Claude
    evaluate_structural_metrics(dataset_path, generated_path)
