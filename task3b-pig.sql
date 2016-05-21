coaches_raw = LOAD 'coaches' 
	using org.apache.hive.hcatalog.pig.HCatLoader(); 
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
coaches = FOREACH coaches_raw 
	GENERATE $0 as coachid, $1 as year, $6 as games, $7 as wins, $8 as lossses, $9 as ties;
ms = FOREACH m GENERATE $1 as coachid, $3 as fname, $4 as lname;
coaches_group = GROUP coaches  ALL;
max_wins = FOREACH coaches_group GENERATE MAX(coaches.wins) as maxwins;
max_wins_coach = JOIN coaches by ($3), max_wins by ($0);
final_pre = JOIN  ms by ($0) ,max_wins_coach by ($0);
final = FOREACH final_pre GENERATE $1 as fname, $2 as lname,$4,$5,$6,$7,$8;--(Scotty,Bowman,1995,82,62,13,7)
DUMP final;