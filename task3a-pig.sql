ac_raw = LOAD 'awardscoaches' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
-- coaches = LOAD 'coaches' 
-- 	using org.apache.hive.hcatalog.pig.HCatLoader();
m = LOAD 'master' 
	using org.apache.hive.hcatalog.pig.HCatLoader();
ac = FOREACH ac_raw GENERATE $0 as coachid, $1 as award, $2 as year;
ms = FOREACH m GENERATE $1 as coachid, $3 as fname, $4 as lname, $19 as byear, $20 as bmon, $21 as bday, $22 as bcountry;
--group and count awards of coach
ac_group = GROUP ac by coachid;
ac_count = FOREACH ac_group GENERATE group as coachid, COUNT(ac) as num_awards;

--sort coach and limit 1
ac_count_order = ORDER ac_count BY num_awards DESC;
ac_max = LIMIT ac_count_order 1; --(irvindi01c, count)
-- DUMP ac_max;
-- join coaches with last result
x = JOIN ac_max by ($0), ms by ($0);--(irvindi01c,9,irvindi01c,Dick,Irvin,1892,7,19,Canada)
final = FOREACH x GENERATE $3 ,$4,$5, $6, $7, $8, $1;
DUMP final;