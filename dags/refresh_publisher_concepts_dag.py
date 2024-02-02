from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

@dag(
    schedule='10 0 14 * *',
    start_date=datetime(2024, 2, 1, 0, 0, 0),
    catchup=False,
    tags=["maint-migrate"],
    template_searchpath="/usr/local/airflow/include",
)
def refresh_publisher_concepts_dag():
    # autcommit=True and split_statements=True are necessary
    # because the SQL script includes "vacuum" statements
    SQLExecuteQueryOperator(task_id="execute_query", 
                            autocommit=True, 
                            split_statements=True, 
                            conn_id="OPENALEX_DB", 
                            sql="refresh-publisher_concepts_mv.sql",
    )

refresh_publisher_concepts_dag()
