--load table
teams_raw = LOAD 'teams' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
scoring_raw = LOAD 'scoring' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
--trim table
teams = FOREACH teams_raw GENERATE $2 as tmid, $13 as points, $18 as teamname;  
allteamsid = FOREACH teams GENERATE $0 as tmid;
teamsid = DISTINCT allteamsid;

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
final = FOREACH final_pre GENERATE $0, $2, $4, $6;

DUMP final;