Task2 a: 
-Hive
SELECT 
	s.gwg as MaxGWG, s.year as year, s.pos,
	m.firstname, m.lastname, m.birthday, m.birthmon, m.birthyear,m.birthcountry
FROM scoring s
JOIN (SELECT max(gwg) as gwg FROM scoring ) max
ON (s.gwg = max.gwg) 
JOIN master m
ON (s.playerid = m.playerid)
JOIN teams t
ON (t.tmid = s.tmid and t.year = s.year);

-Pig

s_raw = LOAD 'scoring' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
t = LOAD 'teams' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
master = FOREACH m GENERATE $0 as playerid, $3 as fname, $4 as lname, $19 as byear, $20 as bmon, $21 as bday, $22 as bcountry;

s_gwg = FOREACH s_raw 
	GENERATE $16 as gwg, $1 as year,$5 as pos, $0 as playerid, $3 as tmid;
s_gwg_group = GROUP s_gwg ALL;--Important
max_gwg = FOREACH s_gwg_group 
	GENERATE  MAX(s_gwg.gwg) as mgwg;

join_gwg = JOIN max_gwg by ($0), s_gwg by ($0);
gwg_2 = FOREACH join_gwg GENERATE $1 as gwg, $2 as year, $3 as pos, $4 as playerid, $5 as tmid; 
join_player = JOIN gwg_2 by ($3), master by ($0);

players = FOREACH join_player GENERATE $0 as gwg, $1 as year, $2 as pos, $3 as playerid, $4 as tmid,$6 as fname, $7 as lname, $8 as byear, $9 as bmon, $10 as bday, $11 as bcountry;
DUMP join_player;

----------------------------------------

