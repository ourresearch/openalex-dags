from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging

logger = logging.getLogger(__name__)


@dag(
    schedule=timedelta(hours=12),
    start_date=datetime(2024, 7, 17, 2, 20, 0),
    catchup=False,
    tags=["logs", "dblogs"],
    template_searchpath="/usr/local/airflow/include",
)
def log_dbcounts_dag():

    @task
    def get_tables_to_log():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """SELECT tablename, schema_name FROM logs.dbcounts_tables_to_track WHERE active IS TRUE AND times_per_day = 2;"""
        db_result = pg_hook.get_records(sq)
        return db_result

    @task
    def count_query(r):
        tablename = r[0]
        schema_name = r[1]
        SQLExecuteQueryOperator(
            task_id=f"execute_query_{schema_name}.{tablename}",
            autocommit=True,
            split_statements=True,
            conn_id="OPENALEX_DB",
            sql="dbcount-parameterized.sql",
            params={"tablename": tablename, "schema_name": schema_name},
        )

    tablenames_query_result = get_tables_to_log()
    # Create, in parallel, one task per result
    count_query.expand(r=tablenames_query_result)


log_dbcounts_dag()
