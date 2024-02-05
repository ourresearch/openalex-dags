from airflow.decorators import task, dag
from airflow.models import Variable
import heroku3
from datetime import datetime, timedelta

# script in openalex-guts to run:
SCRIPT_TO_RUN = "tracking.scripts.add_new_crossref"


@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 2, 0, 0, 0),
    catchup=False,
    tags=["recordtrack"],
)
def track_add_record_from_crossref():
    @task()
    def heroku_run_add_new_crossref():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["openalex-guts"]
        output, dyno = app.run_command(f"python -m {SCRIPT_TO_RUN}")
        return {"output": output}

    result = heroku_run_add_new_crossref()


track_add_record_from_crossref()
