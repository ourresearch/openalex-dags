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
        output, dyno = app.run_command("python -m scripts.update_ror_institutions")
        return {'output': output}
    
    @task()
    def test_sql_select():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """select * from ins.ror_updates order by finished_update_at desc"""
        results = pg_hook.get_records(sq)
        print(f"retrieved {len(results)} records")
        for row in results:
            print(row)

    @task.short_circuit
    def did_update_happen(result):
        output = result['output']
        update_was_skipped = 'exiting without doing any updates' in output.lower()
        return not update_was_skipped


    result = heroku_run_ror_update()
    did_update_happen(result)
    test_sql_select()

test_ror_update()