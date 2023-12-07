from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import heroku3
import requests
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

@dag(
    schedule=timedelta(hours=6),
    start_date=datetime(2023, 9, 25),
    catchup=False,
    tags=["test"],
    template_searchpath="/usr/local/airflow/include",
)
def test_sql():
    SQLExecuteQueryOperator(task_id="execute_query", conn_id="OPENALEX_DB", sql="test.sql")

test_sql()
