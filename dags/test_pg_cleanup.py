from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

@dag(
    schedule=None,
    start_date=datetime(2022, 9, 25),
    catchup=False,
    tags=["test"],
)
def test_pg_cleanup():
    @task()
    def cleanup():
        try:
            pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
            conn = pg_hook.get_conn()
            conn.autocommit = True
            with conn.cursor() as cursor
                sq = """vacuum analyze mid.institution_ancestors_mv"""
                # pg_hook.run(sq)
                cursor.execute(sq)
        finally:
            conn.close()


    cleanup()

test_pg_cleanup()