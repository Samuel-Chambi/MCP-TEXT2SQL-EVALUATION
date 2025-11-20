from prompt_builder import generate_combined_prompts_one

if __name__ == "__main__":
    db_path = "config/db_config.json"
    question = "List all employees with a salary greater than 5000"
    sql_dialect = "MySQL"

    prompt = generate_combined_prompts_one(db_path, question, sql_dialect)
    print(prompt)
