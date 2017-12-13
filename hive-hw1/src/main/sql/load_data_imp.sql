-- Connect
!connect jdbc:hive2://localhost:10000 scott tiger;

-- Load data

-- LOAD DATA INPATH '/data/impression/city.en.txt' OVERWRITE INTO TABLE city;
LOAD DATA INPATH '/data/impression/imp.20131019.txt' OVERWRITE INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131020.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131021.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131022.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131023.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131024.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131025.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131026.txt' INTO TABLE impression;
LOAD DATA INPATH '/data/impression/imp.20131027.txt' INTO TABLE impression;
