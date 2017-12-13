-- Connect
!connect jdbc:hive2://localhost:10000 scott tiger;

-- parseUserAgent (device, os, browser, agent type)

--drop table stats;

-- Temporary table
--create table stats as -- temporary
-- select i.cityid,
--  (parseUserAgent(i.UserAgent)[0]) as device,
--  (parseUserAgent(i.UserAgent)[1]) as os,
--  (parseUserAgent(i.UserAgent)[2]) as browser,
--  (parseUserAgent(i.UserAgent)[3]) as agenttype
-- from impression i;

-- Top device
--drop table devices;
-- create table devices as -- temporary
-- select c.name, s.device, count(*) as cnt from stats s
-- join city c on c.id = s.cityid
-- group by c.name, s.device;
-- select d3.name, d3.device, d3.cnt from devices d3
-- join (select name, max(cnt) as maxcnt from devices group by name) as d2
--     on d2.name = d3.name and d2.maxcnt = d3.cnt;

-- Top os
--drop table oss;
--create table oss as -- temporary
--select c.name, s.os, count(*) as cnt from stats s
--join city c on c.id = s.cityid
--group by c.name, s.os;
--select o3.name, o3.os, o3.cnt from oss o3
--  join (select name, max(cnt) as maxcnt from oss group by name) as o2
--    on o2.name = o3.name and o2.maxcnt = o3.cnt;

-- Top browser
--drop table browsers;
--create table browsers as
--  select c.name, s.browser, count(*) as cnt from stats s
--join city c on c.id = s.cityid
--group by c.name, s.browser;

select b3.name, b3.browser, b3.cnt from browsers b3
  join (select name, max(cnt) as maxcnt from browsers group by name) as b2
    on b2.name = b3.name and b2.maxcnt = b3.cnt;
