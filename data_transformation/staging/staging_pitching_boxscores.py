import os
from sqlalchemy import create_engine, text
from utils.utils import build_db_url

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_PATH = os.path.join(BASE_DIR, "transform_pitching_boxscores.sql")

with open(SQL_PATH, "r") as f:
    SQL = f.read()

engine = create_engine(build_db_url())

def main():
    with engine.begin() as conn:
        conn.execute(text(SQL))

if __name__ == "__main__":
    main()