-- \timing

select * from mid.affiliation limit 1;

select * from authorships.for_disambiguation_work_authors_mv limit 5;


refresh materialized view authorships.for_disambiguation_work_authors_mv;
vacuum analyze authorships.for_disambiguation_work_authors_mv;