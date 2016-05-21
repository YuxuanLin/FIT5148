SELECT
	awardscounts.num_awards, maxcoachid.fname,maxcoachid.lname,maxcoachid.year, maxcoachid.games, maxcoachid.wins, maxcoachid.losses, maxcoachid.ties
FROM
	(SELECT
		maxcoach.coachid as coachid, master.firstname as fname, master.lastname as lname, maxcoach.year as year, maxcoach.games as games,maxcoach.wins as wins,maxcoach.losses as losses,maxcoach.ties as ties
	FROM
		(SELECT
			coachid, year, g as games, w as wins, l as losses , t as ties
		FROM
			coaches
		JOIN
			(SELECT
					max(W) as maxwins
				FROM
					coaches) max
		ON (coaches.w = max.maxwins)
		) maxcoach
	JOIN
		master
	ON
		(maxcoach.coachid = master.coachid)) maxcoachid 
JOIN
	(SELECT 
		count(award) as num_awards,coachid
	FROM awardscoaches
	GROUP BY (coachid)
	) awardscounts
ON (maxcoachid.coachid = awardscounts.coachid);