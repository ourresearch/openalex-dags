from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2024, 4, 17, 20, 0, 0),
    catchup=False,
    tags=["logs"],
    template_searchpath="/usr/local/airflow/include",
)
def log_citation_count_dag():
    SQLExecuteQueryOperator(task_id="execute_query", 
                            autocommit=True, 
                            split_statements=True, 
                            conn_id="OPENALEX_DB", 
                            sql="count-citations.sql",
    )

log_citation_count_dag()
