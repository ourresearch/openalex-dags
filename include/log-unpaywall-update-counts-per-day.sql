with today_count as (
	select count(*) as row_count from ins.unpaywall_recordthresher_fields
	where updated > now() - interval '1d'
)
insert into logs.daily_updates_dbcount (query_timestamp, row_count, tablename, schema_name)
	select now() at time zone 'utc', row_count, 'unpaywall_recordthresher_fields', 'ins'
	from today_count;
