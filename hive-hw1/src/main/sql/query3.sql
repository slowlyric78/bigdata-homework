set hive.exec.parallel=true;

select count(t.cnt) as total, t.loc from (
        select flightnum as cnt, origin as loc from flighthistory where (month in (6,7,8))
        union all
        select flightnum as cnt, dest as loc from flighthistory where (month in (6,7,8))
    ) t
group by t.loc
order by total desc
limit 5;
