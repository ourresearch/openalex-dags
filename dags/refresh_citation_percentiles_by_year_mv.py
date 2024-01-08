from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 7, 3, 15, 0),
    catchup=False,
    tags=["maint-migrate"],
    template_searchpath="/usr/local/airflow/include",
)
def refresh_citation_percentiles_by_year_mv_dag():
    # autcommit=True and split_statements=True are necessary
    # because the SQL script includes "vacuum" statements
    SQLExecuteQueryOperator(task_id="execute_query", 
                            autocommit=True, 
                            split_statements=True, 
                            conn_id="OPENALEX_DB", 
                            sql="refresh-citation-percentiles-by-year-mv.sql",
    )

refresh_citation_percentiles_by_year_mv_dag()
