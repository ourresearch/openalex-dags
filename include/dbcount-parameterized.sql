with count_q as (
	select count(*) as row_count from {{ params.schema_name }}.{{ params.tablename }}
)
insert into logs.dbcount (query_timestamp, row_count, tablename, schema_name)
	select now() at time zone 'utc', row_count, '{{ params.tablename }}', '{{ params.schema_name }}'
	from count_q;
