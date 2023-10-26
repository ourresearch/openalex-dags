from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import heroku3
import requests
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

def get_most_recent_ror_dump_metadata():
    # https://ror.readme.io/docs/data-dump#download-ror-data-dumps-programmatically-with-the-zenodo-api
    url = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    r = requests.get(url)
    if r.status_code >= 400:
        return None
    most_recent_hit = r.json()["hits"]["hits"][0]
    files = most_recent_hit["files"]
    most_recent_file_obj = files[-1]
    return most_recent_file_obj

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2022, 9, 25),
    catchup=False,
    tags=["ROR"],
)
def update_ror_institutions_dag():
    @task.short_circuit
    def check_previous_updates():
        most_recent_file_obj = get_most_recent_ror_dump_metadata()
        if most_recent_file_obj is None:
            raise RuntimeError("Failed to get ROR data. Exiting without doing any updates...")
        pg_hook = PostgresHook(postgres_conn_id="OPENALEX_DB")
        md5_checksum = most_recent_file_obj.get("checksum", "").replace("md5:", "")
        logger.info(f"most recent md5_checksum for ROR data: {md5_checksum}")
        sq = "select md5_checksum, finished_update_at from ins.ror_updates order by finished_update_at desc limit 1"
        db_result = pg_hook.get_records(sq)
        logger.info(db_result)
        if db_result:
            most_recent_openalex_ror_update = db_result[0]
            logger.info(
                f"The most recent ROR update in OpenAlex was: {most_recent_openalex_ror_update[1] or 'DID NOT COMPLETE'}"
            )
            logger.info(
                f"The most recent ROR update in OpenAlex had md5_checksum: {most_recent_openalex_ror_update[0]}"
            )
            if (
                most_recent_openalex_ror_update[0] == md5_checksum
                and most_recent_openalex_ror_update[1] is not None
            ):
                logger.info(
                    f"md5_checksum matches most recent OpenAlex update. This means that ROR data is up to date in OpenAlex. Exiting without doing any updates... (md5_checksum: {md5_checksum})"
                )
                return False
        return True

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


    result = check_previous_updates() >> heroku_run_ror_update()
    did_update_happen(result) >> upsert_into_ror_summary() >> update_existing_rows_in_institution_table() >> insert_new_rows_in_institution_table() >> cleanup()

update_ror_institutions_dag()