from airflow.decorators import task, dag
from airflow.models import Variable
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
        return SQLExecuteQueryOperator(
            task_id="get_tables_to_log",
            sql="SELECT tablename, schema_name FROM logs.dbcounts_tables_to_track WHERE active IS TRUE AND times_per_day = 2;",
        )

    @task
    def count_query(tablename, schema_name):
        SQLExecuteQueryOperator(
            task_id=f"execute_query_{schema_name}.{tablename}",
            autocommit=True,
            split_statements=True,
            conn_id="OPENALEX_DB",
            sql="dbcount-parameterized.sql",
            params={"tablename": tablename, "schema_name": schema_name},
        )

    tablenames_query_result = get_tables_to_log()
    tablenames = [r.tablename for r in tablenames_query_result]
    schema_names = [r.schema_name for r in tablenames_query_result]
    # Create, in parallel, one task per result
    count_query.expand(tablename=tablenames, schema_name=schema_names)


log_dbcounts_dag()
