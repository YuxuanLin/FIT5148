Task2 b:
-Hive:
select 
	counts.awards as awardcount, master.firstname as fname, master.lastname as lname
from 
	(select 
		count(award) as awards, playerid
		from awardsplayers 
		group by playerid) as counts
JOIN 
	(select max(counts.awards) as maxawards
		from
			(select 
				count(x.award) as awards, x.playerid as playerid
				from 
					(SELECT a.award as award, a.playerid as playerid 
					FROM
						awardsplayers a 
					JOIN
						(SELECT 
							DISTINCT m.playerid, m.firstname, m.lastname
						FROM scoring s
						JOIN (SELECT max(gwg) as gwg FROM scoring ) max
						ON (s.gwg = max.gwg) 
						JOIN master m
						ON (s.playerid = m.playerid)) k
					ON (a.playerid = k.playerid)) x 
				group by x.playerid) as counts) as max
ON (counts.awards = max.maxawards)
JOIN 
	master
ON (master.playerid = counts.playerid)
;


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

players = FOREACH join_player GENERATE  $3 as playerid, $6 as fname, $7 as lname;

players_d = DISTINCT players;
--players: GWG最高的选手----------------------------------
ap_raw = LOAD 'awardsplayers' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
ap_raw2 = FOREACH ap_raw GENERATE $0 as playerid, $1 as award;--Awards表

ap_group = GROUP ap_raw2 BY playerid;
ap_group_count = FOREACH ap_group GENERATE COUNT(ap_raw2) as awardscount, ap_raw2.playerid as playerid;
C = FOREACH ap_group_count GENERATE $0 as awardscount, FLATTEN($1);
X = ORDER C BY $0 DESC;
M = JOIN X BY ($1), players_d BY ($0);
Z = LIMIT M 1;
N =	FOREACH Z GENERATE $0 AS awardcount, $3,  $4;
DUMP N;












