select count(uniquecarrier), uniquecarrier from flighthistory
where year = 2007 group by uniquecarrier;