-- Load data into Hive tables

LOAD DATA INPATH '/data/flights/airports.csv' OVERWRITE INTO TABLE airport;
LOAD DATA INPATH '/data/flights/carriers.csv' OVERWRITE INTO TABLE carrier;
LOAD DATA INPATH '/data/flights/2007.csv' OVERWRITE INTO TABLE flighthistory;