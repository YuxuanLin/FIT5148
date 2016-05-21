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
finalTeams = FOREACH final_pre GENERATE $0, $2, $4, $6;--(VMR,34,59,155)

scoring_raw = LOAD 'scoring' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
team_raw = LOAD 'teams' 
	using org.apache.hive.hcatalog.pig.HCatLoader();

scoring = FOREACH scoring_raw GENERATE $0 as playerid, $3 as tmid , $9 as pts;
teams = FOREACH team_raw GENERATE $2 as tmid, $18 as teamname;
--以tmid分组
scoring_tm = GROUP scoring by (tmid,playerid);

scoring_sum = FOREACH scoring_tm GENERATE group as  tmidNplayerid, SUM(scoring.pts) as pts;
sumPre = FOREACH scoring_sum GENERATE FLATTEN($0), $1 as pts;--(ALB,norrija01,3)
sumPre2 = FOREACH sumPre GENERATE $0 as tmid, $1 as playerid, $2 as pts;--(ALB,norrija01,3)
sum_grp = GROUP sumPre2 by tmid;
maxInTm = FOREACH sum_grp GENERATE group as tmid,  MAX(sumPre2.pts); 
s1 = JOIN maxInTm by ($0, $1), sumPre2 by ($0, $2);--(CAC,139,CAC,lawsoda01,139)
final_preX = FOREACH s1 GENERATE $0 as tmid, $1 as pts, $3 as playerid;--(tmid, pts, playerid)
--join team to get team name;
--join master to get fname adn lname, year
final_pre2 = JOIN final_preX by ($0), finalTeams by ($0);
--(TOT,207,kirkga01,TOT,227,1570,988)
pre3 = JOIN final_pre2 by ($0),  teams by ($0);
--(teamname, fname, lname, year, pts, goal, assists)

DUMP pre3;

--得到每组最大pts 和其playerid
