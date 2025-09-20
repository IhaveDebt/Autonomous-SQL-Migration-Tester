"""
Autonomous SQL Migration Tester
Single-file program demonstrating sandboxed migration testing with risk reports.
"""

import psycopg
import os
import time

DB_URL = "postgresql://postgres:postgres@localhost:5432/migration_test_db"
MIGRATIONS_DIR = "migrations"

def init_db():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sample_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INT
            );
            """)
            cur.execute("""
            INSERT INTO sample_table (name, value)
            SELECT 'Item'||i, i*10 FROM generate_series(1,10) AS s(i)
            ON CONFLICT DO NOTHING;
            """)

def run_migration(file_path):
    with open(file_path, "r") as f:
        sql = f.read()
    start_time = time.time()
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    end_time = time.time()
    return end_time - start_time

def run_test_queries():
    queries = [
        "SELECT COUNT(*) FROM sample_table;",
        "SELECT SUM(value) FROM sample_table;",
        "SELECT * FROM sample_table WHERE value > 30;"
    ]
    results = []
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
                results.append(cur.fetchall())
    return results

def compare_results(old, new):
    report = []
    for i, (o, n) in enumerate(zip(old,new)):
        if o != n:
            report.append(f"Query {i+1} result differs: {o} -> {n}")
    return report

if __name__ == "__main__":
    init_db()
    print("Autonomous SQL Migration Tester\n")
    old_results = run_test_queries()
    for migration_file in sorted(os.listdir(MIGRATIONS_DIR)):
        path = os.path.join(MIGRATIONS_DIR, migration_file)
        print(f"Running migration: {migration_file}")
        duration = run_migration(path)
        new_results = run_test_queries()
        diff_report = compare_results(old_results, new_results)
        print(f"Migration duration: {duration:.2f}s")
        if diff_report:
            print("Differences found:")
            for r in diff_report:
                print(f"* {r}")
        else:
            print("No differences detected.")
        print("-"*40)
