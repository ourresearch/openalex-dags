from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging

logger = logging.getLogger(__name__)


@dag(
    schedule=timedelta(hours=12),
    start_date=datetime(2024, 7, 18, 7, 20, 0),
    catchup=False,
    tags=["logs", "dblogs"],
    template_searchpath="/usr/local/airflow/include",
)
def log_dbcounts_freetext_queries_dag():

    @task
    def get_queries_to_log():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """SELECT id, text_query FROM logs.dbcount_freetext_queries_to_track WHERE active IS TRUE AND times_per_day = 2;"""
        db_result = pg_hook.get_records(sq)
        return db_result

    @task
    def count_query(r):
        query_id = r[0]
        text_query = r[1]
        logger.info(f"(query_id: {query_id}): {text_query}")
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        count_result = pg_hook.get_records(text_query)
        row_count = count_result[0][0]
        sq = f"""insert into logs.dbcount_freetext_queries (query_timestamp, query_id, row_count)
	select now() at time zone 'utc', {query_id}, {row_count};"""
        pg_hook.run(sq)

    tablenames_query_result = get_queries_to_log()
    # Create, in parallel, one task per result
    count_query.expand(r=tablenames_query_result)


log_dbcounts_freetext_queries_dag()
