--load table
teams_raw = LOAD 'teams' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
scoring_raw = LOAD 'scoring' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
--trim table
teams = FOREACH teams_raw GENERATE $2 as tmid, $13 as points, $18 as teamname;  
allteamsid = FOREACH teams GENERATE $0 as tmid;
teamsid = DISTINCT allteamsid;
teamsName_pre = FOREACH teams GENERATE $0 as tmid, $2 as teamname;
teamsName = DISTINCT teamsName_pre;
master  = FOREACH m GENERATE $0 as playerid, $3 as fname, $4 as lname;
-- DUMP A_unique;
scoring = FOREACH scoring_raw GENERATE $3 as tmid, $7 as goals, $8 as assists;
--aggregate points by tmid from table team
teams_gb_tmid = GROUP teams by tmid;
teamSumPoints =	FOREACH teams_gb_tmid GENERATE group as tmid, SUM(teams.points) as sumpoints;--(ANA,427)
--aggregate assists by tmid from table scoring
scoring_bg_tmid = GROUP scoring by tmid;
assistsSum_scoring = FOREACH scoring_bg_tmid GENERATE group as tmid, SUM(scoring.assists) as sumassists;--(ANA,4152)
-- DUMP assistsSum_scoring;
--aggregate goals by tmid from table scoring
goalsSum_scoring = FOREACH scoring_bg_tmid GENERATE group as tmid, SUM(scoring.goals) as sumgoals;--(ANA,2471)

final_pre = JOIN teamsid by ($0), teamSumPoints by ($0), assistsSum_scoring by ($0), goalsSum_scoring by ($0);--(VMR,VMR,34,VMR,59,VMR,155)
finalTeams = FOREACH final_pre GENERATE $0, $2, $4, $6;--(VMR,34,59,155)

--Trim table
scoring2 = FOREACH scoring_raw GENERATE $0 as playerid, $3 as tmid , $9 as pts, $1 as year, $7 as goals,$8 as assists;
teams2 = FOREACH teams_raw GENERATE $2 as tmid, $18 as teamname;
--Group by tmid
scoring_tm = GROUP scoring2 by (tmid);
scoring_max = FOREACH scoring_tm GENERATE group as tmid, MAX(scoring2.pts) as pts;
x = JOIN scoring_max by ($0, $1), scoring2 by ($1, $2);--(ALB,86,harriji01,ALB,86,1972,39,47)
m = FOREACH x GENERATE $0 as tmid, $1 as pts, $2 as plyarid, $5 as year, $6 as G, $7 as A;--(ALB,86,harriji01,1972,39,47)

--JOIN teams 
m_tn = JOIN m by ($0), teamsName by ($0);--(DOT,50,backsra01,1975,21,29,DOT,Denver Spurs/Ottawa Civics)
-- DUMP m_tn;
m_xx = FOREACH m_tn GENERATE $0 as tmid, $1 as pts, $2 as pid, $3 as year, $4 as G, $5 as A, $7 as tname;----(DOT,50,backsra01,1975,21,29,Denver Spurs/Ottawa Civics)

--and master
final2_pre= JOIN m_xx by ($2), master by ($0);--(TRS,46,dyeba01,1924,38,8,Toronto St. Patricks,dyeba01,Babe,Dye)
final = FOREACH final2_pre GENERATE $6 as teamname,$8 as fname, $9 as lname, $3 as year,$1 as pts, $4 as G, $5 as A;
DUMP final;

