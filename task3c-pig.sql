--load table
coaches_raw = LOAD 'coaches' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
--Trim table
coaches = FOREACH coaches_raw 
	GENERATE $0 as coachid, $1 as year, $6 as games, $7 as wins, $8 as lossses, $9 as ties;
ms = FOREACH m GENERATE $1 as coachid, $3 as fname, $4 as lname;
--Group to find max wins
coaches_group = GROUP coaches  ALL;
max_wins = FOREACH coaches_group GENERATE MAX(coaches.wins) as maxwins;

--Find coach with max wins
max_wins_coach = JOIN coaches by ($3), max_wins by ($0);

--Join to find other information about the max wins' coach
final_pre = JOIN  ms by ($0) ,max_wins_coach by ($0);
maxwinscoah = FOREACH final_pre GENERATE $1 as fname, $2 as lname ,$3,$4,$5,$6,$7,$8;--(Scotty,Bowman,bowmasc01c,1995,82,62,13,7)


ac_raw = LOAD 'awardscoaches' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
ac = FOREACH ac_raw GENERATE $0 as coachid, $1 as award, $2 as year;
ms = FOREACH m GENERATE $1 as coachid, $3 as fname, $4 as lname, $19 as byear, $20 as bmon, $21 as bday, $22 as bcountry;
--group and count awards of coach
ac_group = GROUP ac by coachid;
ac_count = FOREACH ac_group GENERATE group as coachid, COUNT(ac) as num_awards;

--Find the awards of this max wins' coach;
awards_coach = JOIN ac_count by ($0), maxwinscoah by ($2);--(bowmasc01c,2,Scotty,Bowman,bowmasc01c,1995,82,62,13,7)
final_awards_coach = FOREACH awards_coach GENERATE $1 as awards, $2 as fname, $3 as lname, $5 as year, $6, $7,$8,$9;
DUMP final_awards_coach;


