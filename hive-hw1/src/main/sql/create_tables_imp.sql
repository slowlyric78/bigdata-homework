-- Connect
!connect jdbc:hive2://localhost:10000 scott tiger;

-- Create impression tables

CREATE TABLE City (id int, name string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "\t", "quoteChar"     = "\"")
STORED AS TEXTFILE;

CREATE TABLE Impression (BidID string, timestmp string, LogType tinyint, iPinYouID string, UserAgent string, IP string, RegionID int, CityID int, AdExchange tinyint, Domain string, URL string, AnonymousURL string, AdSlotID string, AdSlotWidth int, AdSlotHeight int, AdSlotVisibility string, AdSlotFormat string, AdSlotFloorPrice int, CreativeID string, BiddingPrice int, PayingPrice int, LandingPageURL string, AdertiserID int, UserProfileIDs string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "\t", "quoteChar"     = "\"")
STORED AS TEXTFILE;