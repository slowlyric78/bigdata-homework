set hive.exec.parallel=true;

select count(fh.flightnum) as total, c.description from flighthistory fh
join carrier c on c.code = fh.uniquecarrier
where fh.year = 2007 and fh.cancelled = 0
group by fh.uniquecarrier, c.description
order by total desc
limit 5;
