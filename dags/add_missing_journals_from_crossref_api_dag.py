from airflow.decorators import task, dag
from airflow.models import Variable
import heroku3
from datetime import datetime, timedelta

# script in openalex-guts to run:
SCRIPT_TO_RUN = "cleanup.scripts.add_missing_journals_from_crossref_api"


@dag(
    schedule=timedelta(days=7),
    start_date=datetime(2023, 10, 2),
    catchup=False,
)
def add_missing_journals_from_crossref_api_dag():
    @task()
    def heroku_run_missing_crossref_journals():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["openalex-guts"]
        output, dyno = app.run_command(f"python -m {SCRIPT_TO_RUN}")
        return {"output": output}

    result = heroku_run_missing_crossref_journals()


add_missing_journals_from_crossref_api_dag()
