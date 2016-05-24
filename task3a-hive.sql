SELECT
	pre.maxawards, master.firstname, master.lastname, master.birthyear, master.birthmon, master.birthday, master.birthcountry
FROM
	(SELECT max.maxawards as maxawards, awardscounts_2.coachid as coachid
	FROM 
		(SELECT count(award) as num_awards,coachid
		FROM awardscoaches
		GROUP BY (coachid)
		) awardscounts_2
	JOIN
		(SELECT MAX(awardscounts.num_awards) as maxawards
		FROM
			(SELECT count(award) as num_awards,coachid
			FROM awardscoaches
			GROUP BY (coachid)
			) awardscounts
		) max
	ON (awardscounts_2.num_awards = max.maxawards)
	) pre
JOIN master
ON ( pre.coachid = master.coachid)
;


--Analysis
ANALYZE TABLE awardscoaches COMPUTE STATISTICS;
ANALYZE TABLE awardscoaches COMPUTE STATISTICS FOR COLUMNS coachid, award;
ANALYZE TABLE master COMPUTE STATISTICS;
ANALYZE TABLE master COMPUTE STATISTICS FOR COLUMNS firstname, lastname, birthyear, birthmon, birthday, birthcountry;