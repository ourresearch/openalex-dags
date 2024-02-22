from airflow.decorators import task, dag
from airflow.models import Variable
import heroku3
from datetime import datetime, timedelta

# script in oadoi to run:
SCRIPT_TO_RUN = "tracking.scripts.arxiv_oai_pmh_add_tracking_records"


@dag(
    schedule=timedelta(hours=12),
    start_date=datetime(2024, 1, 2, 0, 0, 0),
    catchup=False,
    tags=["recordtrack"],
)
def track_add_records_from_arxiv():
    @task()
    def heroku_run_add_new_arxiv():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["oadoi"]
        output, dyno = app.run_command(f"python -m {SCRIPT_TO_RUN}")
        return {"output": output}

    result = heroku_run_add_new_arxiv()


track_add_records_from_arxiv()
