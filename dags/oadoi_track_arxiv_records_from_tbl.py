from airflow.decorators import task, dag
from airflow.models import Variable
import heroku3
from datetime import datetime, timedelta

# script in oadoi to run:
SCRIPT_TO_RUN = "tracking.scripts.track_arxiv_records"


@dag(
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 2),
    catchup=False,
    tags=["recordtrack"],
)
def oadoi_track_arxiv_records_from_tbl_dag():
    @task()
    def heroku_run_track_arxiv_records_from_tbl():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["oadoi"]
        output, dyno = app.run_command(f"python -m {SCRIPT_TO_RUN}")
        return {"output": output}

    result = heroku_run_track_arxiv_records_from_tbl()


oadoi_track_arxiv_records_from_tbl_dag()
