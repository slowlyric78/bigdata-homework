select count(*)  from flighthistory fh
join airport a1 on a1.iata = fh.origin
join airport a2 on a2.iata = fh.dest
where year = 2007 and month = 6 and (a1.city = 'New York' or a2.city = 'New York')
