from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

import logging
logger = logging.getLogger(__name__)


@dag(
    schedule='0 4 * * 2',  # every Tuesday at 4:00 UTC
    start_date=datetime(2024, 5, 7, 0, 0, 0),
    catchup=False,
    tags=["maint-migrate"],
    template_searchpath="/usr/local/airflow/include",
)
def refresh_citation_authors_by_year_citation_count_mv_dag():
    # autcommit=True and split_statements=True are necessary
    # because the SQL script includes "vacuum" statements
    SQLExecuteQueryOperator(task_id="execute_query",
                            autocommit=True,
                            split_statements=True,
                            conn_id="OPENALEX_DB",
                            sql="refresh-citation_authors_by_year_citation_count_mv.sql",
    )

refresh_citation_authors_by_year_citation_count_mv_dag()
