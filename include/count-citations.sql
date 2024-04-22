with citcounts as (
	select count(*) as num_citations from mid.citation
)
insert into logs.citation_count (query_timestamp, num_citations)
	select now() at time zone 'utc', num_citations
	from citcounts;
