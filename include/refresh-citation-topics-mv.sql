refresh materialized view concurrently mid.citation_topics_mv;
vacuum analyze mid.citation_topics_mv;

refresh materialized view concurrently mid.citation_subfields_mv;
vacuum analyze mid.citation_subfields_mv;

refresh materialized view concurrently mid.citation_fields_mv;
vacuum analyze mid.citation_fields_mv;

refresh materialized view concurrently mid.citation_domains_mv;
vacuum analyze mid.citation_domains_mv;