!connect jdbc:hive2://localhost:10000 scott tiger;

-- drop table cancellation;

create temporary table cancellation as
select count(*) as cnt, fh.uniquecarrier as airline, a.city
from flighthistory fh
join airport a on a.iata = fh.origin
where fh.cancelled = 1 and fh.year = 2007
group by fh.uniquecarrier, a.city;

-- select cr.description, sum(c.cnt) as cnt1, concat_ws(',',collect_set(c.city))
select c.airline, cr.description, sum(c.cnt) as cnt1, concat_ws(',',collect_set(c.city))
from cancellation c
join carrier cr on cr.code = c.airline
group by c.airline, cr.description
order by cnt1 desc;
