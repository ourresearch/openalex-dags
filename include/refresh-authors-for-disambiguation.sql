-- ingest newly disambiguated authors

-- create temp table tmp_authorships_authors_modified as (select * from authorships.authors_modified 
-- 	where modified_date > now() - interval '6 hours'
-- );
-- alter table tmp_authorships_authors_modified add column paper_id bigint generated always as (split_part(work_author_id, '_', 1)::bigint) stored;
-- alter table tmp_authorships_authors_modified add column author_sequence_number integer generated always as (split_part(work_author_id, '_', 2)::integer) stored;
--alter table tmp_authorships_authors_modified add column rand double precision default random();
-- create index on tmp_authorships_authors_modified (author_id_changed);
-- create index on tmp_authorships_authors_modified (author_id);
--create index on tmp_authorships_authors_modified (rand) where author_id_changed;

-- update affiliation author_ids
-- update mid.affiliation 
-- set author_id = t.author_id,
-- updated_date = now()
-- from tmp_authorships_authors_modified t 
-- where t.paper_id = affiliation.paper_id 
-- and t.author_sequence_number = affiliation.author_sequence_number 
-- and t.author_id_changed 
-- and affiliation.author_id is distinct from t.author_id;

-- store works with updated authors
-- insert into authorships.works_to_enqueue (paper_id) (
--   select distinct paper_id from tmp_authorships_authors_modified
-- ) on conflict do nothing;

-- get updated author properties
-- create temp table tmp_authorships_authors_modified_properties as (
--     select author_id, display_name, alternate_names, orcid, created_date, modified_date, author_id_changed from (
--         select *, row_number() over (partition by author_id) as rn from tmp_authorships_authors_modified
--     ) x 
--     where rn = 1
-- );

-- create unique index on tmp_authorships_authors_modified_properties (author_id);
-- analyze tmp_authorships_authors_modified_properties;

-- upsert authors

-- create temp table authors_info as (
--   select
--     author_id,
--     display_name,
--     coalesce(modified_date, now()) as modified_date
--   from
--     tmp_authorships_authors_modified_properties
-- );

-- analyze authors_info;

-- update mid.author 
-- set display_name = authors_info.display_name,
-- updated_date = greatest(
--   coalesce(author.updated_date, '1970-01-01'::timestamp without time zone),
--   authors_info.modified_date
-- )
-- from authors_info
-- where author.author_id = authors_info.author_id
-- and (
--   author.display_name is distinct from authors_info.display_name
--   or authors_info.modified_date > coalesce(author.updated_date, '1970-01-01'::timestamp without time zone)
-- );

-- insert into mid.author (author_id, display_name, updated_date, created_date) (
--   select authors_info.*, now() from authors_info left join mid.author using (author_id) where author.author_id is null
-- );

-- update alternate names
-- delete from legacy.mag_main_author_extended_attributes
-- where author_id in (select author_id from tmp_authorships_authors_modified_properties);

-- insert into legacy.mag_main_author_extended_attributes (
--     select author_id, 1 as attribute_type, unnest(alternate_names) as attribute_value 
--     from tmp_authorships_authors_modified_properties
-- );

-- update orcids
-- delete from mid.author_orcid 
-- where author_id in (select author_id from tmp_authorships_authors_modified_properties) 
-- and evidence = 'AND_v3';

-- insert into mid.author_orcid (
--     select author_id, orcid, now(), 'AND_v3' 
--     from tmp_authorships_authors_modified_properties 
--     where orcid is not null and trim(orcid) != ''
-- );

-- update latest affiliations

-- create temp table tmp_authorships_modified_latest_works as (
--     select 
--         author_id,
--         updated_date,
--         affiliation_id as last_known_affiliation_id,
--         publication_date::timestamp without time zone as last_known_affiliation_id_date 
--     from (
--         select 
--             *,
--             row_number() over (
--                 partition by author_id
--                 order by publication_date desc nulls last, updated_date desc nulls last, affiliation_id desc nulls last
--             ) as rn 
--         from (
--             select 
--                 author.author_id,
--                 af.updated_date,
--                 af.affiliation_id,
--                 w.publication_date
--             from 
--                 (select distinct author_id from tmp_authorships_authors_modified_properties where author_id_changed) author
--                 left join mid.affiliation af
--                 on af.author_id = author.author_id and affiliation_id is not null
--                 left join mid.work w on w.paper_id = af.paper_id
--         ) x
--     ) y 
--     where rn = 1
-- );

-- update mid.author
-- set last_known_affiliation_id = t.last_known_affiliation_id,
-- last_known_affiliation_id_date = t.last_known_affiliation_id_date
-- from tmp_authorships_modified_latest_works t
-- where t.author_id = author.author_id and t.last_known_affiliation_id is distinct from author.last_known_affiliation_id;


-- update authorships that still need disambiguation
-- refresh materialized view authorships.for_disambiguation_work_authors_mv;
-- vacuum analyze authorships.for_disambiguation_work_authors_mv;
-- refresh materialized view authorships.for_disambiguation_concepts_mv;
-- vacuum analyze authorships.for_disambiguation_concepts_mv;
-- refresh materialized view authorships.for_disambiguation_citations_mv;
-- vacuum analyze authorships.for_disambiguation_citations_mv;
-- refresh materialized view authorships.for_disambiguation_coauthors_mv;
-- vacuum analyze authorships.for_disambiguation_coauthors_mv;
-- refresh materialized view authorships.for_disambiguation_mv;
-- vacuum analyze authorships.for_disambiguation_mv;

