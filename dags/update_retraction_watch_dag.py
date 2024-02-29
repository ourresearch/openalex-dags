from airflow.decorators import task, dag
from airflow.models import Variable
import heroku3
from datetime import datetime, timedelta

# script in openalex-guts to run:
SCRIPT_TO_RUN = "scripts.update_retraction_watch"


@dag(
    schedule=timedelta(days=7),
    start_date=datetime(2024, 2, 7, 6, 0, 0),
    catchup=False,
)
def update_retraction_watch_dag():
    @task()
    def heroku_run_update_retraction_watch():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["openalex-guts"]
        output, dyno = app.run_command(f"python -m {SCRIPT_TO_RUN}")
        return {"output": output}

    result = heroku_run_update_retraction_watch()


update_retraction_watch_dag()
