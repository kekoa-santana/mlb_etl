import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy.engine import URL

load_dotenv(find_dotenv(), override=False)

def build_db_url(
    db_user: str='kekoa', 
    host: str='localhost',
    port: str='5433',
    database: None = None
    ) -> URL:
    if database == None:
        print('Database param needs to be set')
    return URL.create(
        "postgresql+psycopg",
        username=os.getenv("DB_USER", db_user),
        password=os.getenv("DB_PASSWORD", " "),
        host=os.getenv("DB_HOST", host),
        port=int(os.getenv("DB_PORT", port)),
        database=os.getenv("DB_NAME", database),
    )