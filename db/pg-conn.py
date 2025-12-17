import os
import psycopg
import logging

PG_DSN = os.getenv("DATABASE_URI")

pg_conn = psycopg.connect(PG_DSN)
pg_conn.autocommit = True
pg_conn.execute("SET timezone = 'UTC'")

logging.info("✅ PostgreSQL connection initialized")
