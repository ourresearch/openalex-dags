create temp table tmp_unmapped_by_doi as (
  select r.id, w.paper_id as work_id, r.updated, floor(20*random()) as block
  from ins.recordthresher_record r 
  join mid.work w 
  on w.doi_lower = r.doi 
  and r.work_id is null
  and w.merge_into_id is null
);
create index on tmp_unmapped_by_doi (block);
analyze tmp_unmapped_by_doi;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 0;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 1;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 2;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 3;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 4;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 5;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 6;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 7;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 8;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 9;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 10;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 11;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 12;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 13;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 14;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 15;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 16;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 17;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 18;
update ins.recordthresher_record r set work_id = u.work_id from tmp_unmapped_by_doi u where u.id = r.id and r.work_id is null and u.block = 19;
insert into queue.run_once_work_add_everything (work_id, work_updated) (
    select work_id, updated from tmp_unmapped_by_doi
) on conflict do nothing;