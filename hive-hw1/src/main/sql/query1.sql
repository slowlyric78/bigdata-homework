select count(uniquecarrier), uniquecarrier from flighthistory
where year = 2007 and cancelled = 0
group by uniquecarrier;