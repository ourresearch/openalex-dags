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
def update_ror_institutions_dag():
    @task()
    def cleanup():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """vacuum analyze mid.institution_ancestors_mv"""
        pg_hook.run(sq)


    cleanup()

update_ror_institutions_dag()