from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

@dag(
    schedule=timedelta(hours=6),
    start_date=datetime(2023, 9, 25),
    catchup=False,
    tags=["maint-migrate"],
    template_searchpath="/usr/local/airflow/include",
)
def refresh_authors_for_disambiguation_dag():
    # autcommit=True and split_statements=True are necessary
    # because the SQL script includes "vacuum" statements
    SQLExecuteQueryOperator(task_id="execute_query", 
                            autocommit=True, 
                            split_statements=True, 
                            conn_id="OPENALEX_DB", 
                            sql="refresh-authors-for-disambiguation.sql",
    )

refresh_authors_for_disambiguation_dag()
