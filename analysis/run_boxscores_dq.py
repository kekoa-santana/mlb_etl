from sqlalchemy import create_engine, text
from utils.utils import build_db_url
from data_quality.generate_numeric_checks import run_numeric_checks
import os

def get_sql_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)

def run_checks(sql_file_path: str):
    engine = create_engine(build_db_url())

    with open(sql_file_path, "r") as f:
        sql = f.read()

    queries = [q.strip() for q in sql.split(";") if q.strip()]

    with engine.connect() as conn:
        for i, query in enumerate(queries, start=1):
            print(f"\n------------- Running check {i} -------------")
            print(query)
            print("-----------------------------------------------")

            try:
                result = conn.execute(text(query))
                rows = result.fetchall()
                print(f"Returned {len(rows)} rows")

                for row in rows:
                    print(row)

            except Exception as e:
                print(f"[ERROR] Check {i} failed: {str(e)}")

if __name__ == "__main__":
    sql_paths = get_sql_path("raw_pitching_boxscore_checks.sql")
    run_checks(sql_paths)
    run_numeric_checks()
