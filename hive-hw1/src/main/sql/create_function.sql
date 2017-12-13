-- Connect
!connect jdbc:hive2://localhost:10000 scott tiger;


-- Clean update
drop function parseUserAgent;

-- Setup function
-- add jar hdfs:///udf/hive-hw3-1.0-SNAPSHOT.jar;
-- add jar hdfs:///udf/UserAgentUtils-1.21-SNAPSHOT.jar;
create function parseUserAgent as 'com.epam.hive.UserAgentUDF' using jar "hdfs:///udf/hive-hw3-1.0-SNAPSHOT.jar", jar "hdfs:///udf/UserAgentUtils-1.21-SNAPSHOT.jar";