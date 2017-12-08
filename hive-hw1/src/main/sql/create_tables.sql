-- Code,Description
-- "02Q","Titan Airways"
CREATE TABLE Carrier (code string, description string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"")
STORED AS TEXTFILE tblproperties("skip.header.line.count"="1");
-- row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");

-- "iata","airport","city","state","country","lat","long"
-- "00M","Thigpen ","Bay Springs","MS","USA",31.95376472,-89.23450472
CREATE TABLE Airport (iata string, airport string, city string, state string, country string, lat double, long double)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar"     = "\"")
STORED AS TEXTFILE tblproperties("skip.header.line.count"="1");

-- Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
-- 2007,1,1,1,1232,1225,1341,1340,WN,2891,N351,69,75,54,1,7,SMF,ONT,389,4,11,0,,0,0,0,0,0,0
CREATE TABLE FlightHistory (year int, month int, dayofmonth int, dayofweek int, deptime string, crsdeptime string, arrtime string, crsarrtime string, uniquecarrier string, flightnum string, tailnum string, actualelapsetime int, crselapsedtime int, airtime int, arrdelay int, depdelay int, origin string, dest string, distance int, taxin int, taxiout int, cancelled int, cancellationcode string, diverted int, carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lataircraftdelay int) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");