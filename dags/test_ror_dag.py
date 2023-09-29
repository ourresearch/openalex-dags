from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import heroku3
import pendulum

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    tags=["test"],
)
def test_ror_update():
    @task()
    def heroku_run_ror_update():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["openalex-guts"]
        output = app.run_command("python -m scripts.update_ror_institutions")
        return output

    output = heroku_run_ror_update()

test_ror_update()