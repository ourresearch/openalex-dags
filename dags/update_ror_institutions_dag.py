from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import heroku3
from datetime import datetime, timedelta

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2022, 9, 25),
    catchup=False,
    tags=["ROR"],
)
def update_ror_institutions_dag():
    @task()
    def heroku_run_ror_update():
        heroku_api_key = Variable.get("HEROKU_API_KEY")
        heroku_conn = heroku3.from_key(heroku_api_key)
        app = heroku_conn.apps()["openalex-guts"]
        output, dyno = app.run_command("python -m scripts.update_ror_institutions", size="standard-2x")
        update_was_skipped = 'exiting without doing any updates' in output.lower()
        if update_was_skipped and 'failed' in output.lower():
            raise RuntimeError("There was a problem while attempting the ROR update")
        return {'output': output}

    @task()
    def upsert_into_ror_summary():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """
        insert into ins.ror_summary
        select * from ins.ror_summary_view
        on conflict (ror_id) do
        update set
        	"name"=excluded."name",
        	official_page=excluded.official_page,
        	wikipedia_url=excluded.wikipedia_url,
        	grid_id=excluded.grid_id,
        	latitude=excluded.latitude,
        	longitude=excluded.longitude,
        	city=excluded.city,
        	state=excluded.state,
        	country=excluded.country,
        	country_code=excluded.country_code,
        	ror_type=excluded.ror_type
        ;
        """
        pg_hook.run(sq)
    
    @task()
    def update_existing_rows_in_institution_table():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """
        update mid.institution i set
        	display_name = ror."name",
        	grid_id = ror.grid_id,
        	official_page = ror.official_page,
        	wiki_page = ror.wikipedia_url,
        	iso3166_code = ror.country_code,
        	latitude = ror.latitude,
        	longitude = ror.longitude,
        	city = ror.city,
            region = ror.state,
        	country = ror.country, 
        	updated_date = now() at time zone 'utc'
        from ins.ror_summary ror
        where i.ror_id = ror.ror_id; 
        """
        pg_hook.run(sq)
    
    @task()
    def insert_new_rows_in_institution_table():
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        sq = """
        with insert_rows as (
        select rs.*, i.affiliation_id  
        from ins.ror_summary rs 
        left join mid.institution i 
        	on rs.ror_id = i.ror_id 
        where i.affiliation_id is null
        )
        insert into mid.institution (display_name, official_page, wiki_page, iso3166_code, latitude, longitude, grid_id, ror_id, city, region, country, created_date, updated_date)
        select name, official_page, wikipedia_url, country_code, latitude, longitude, grid_id, ror_id, city, state, country, now() at time zone 'utc', now() at time zone 'utc'
        from insert_rows;
        """
        pg_hook.run(sq)
    
    @task.short_circuit
    def did_update_happen(result):
        output = result['output']
        update_was_skipped = 'exiting without doing any updates' in output.lower()
        return not update_was_skipped

    @task()
    def cleanup():
        try:
            pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
            conn = pg_hook.get_conn()
            # vacuum command needs to be run with autocommit = True
            conn.autocommit = True
            with conn.cursor() as cursor:
                sq = """vacuum analyze mid.institution_ancestors_mv"""
                cursor.execute(sq)
        finally:
            conn.close()


    result = heroku_run_ror_update()
    did_update_happen(result) >> upsert_into_ror_summary() >> update_existing_rows_in_institution_table() >> insert_new_rows_in_institution_table() >> cleanup()

update_ror_institutions_dag()