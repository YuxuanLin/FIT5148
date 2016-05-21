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
Z = LIMIT M 1;-- Z: (17,esposph01,esposph01,Phil,Esposito)
N =	FOREACH Z GENERATE $0 AS awardcount, $3,  $4;-- N: (17,Phil,Esposito)
p_id_join = JOIN N BY ($1, $2), m BY ($3, $4);--($3 ID)
-- Find playerID from Master by firstname and lastname;
p_id = FOREACH p_id_join GENERATE $3 as playerid; --p_id: (esposph01)
ap_year = FOREACH ap_raw GENERATE $0 as playerid, $1 as award, $2 as year;
-- Find all awards, year(distinct) from AwardsPlayers table with the playerID;
award_join = JOIN p_id by ($0), ap_year by ($0); --(esposph01,esposph01,First Team All-Star,1969)
-- DUMP award_join;
scoring = FOREACH s_raw GENERATE   $0 as playerid, $1 as year, $9 as pts; --scoring:(playerid, year,pts)
scoring_filter_raw = JOIN scoring by ($0), p_id by($0);--scoring_filter(esposph01,1975,67,esposph01)
-- SUM points by year and id from scoring_filter;
scoring_filter = FOREACH scoring_filter_raw GENERATE $0 as playerid, $1 as year, $2 as pts;--scoring_filter(esposph01,1975,67)
scoring_filter_gb_year = GROUP scoring_filter BY year;
sum_scoring_by_year = FOREACH scoring_filter_gb_year GENERATE group as year, SUM(scoring_filter.pts) as sumpts;
final_raw = JOIN award_join by ($3), sum_scoring_by_year by ($0);--(esposph01,esposph01,Second Team All-Star,1967,1967,84)
final = FOREACH final_raw GENERATE $2 as award, $3 as year, $5 as pts;
final_o = ORDER final BY year;
DUMP final_o;














