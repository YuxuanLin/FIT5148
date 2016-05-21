SELECT
	master.firstname, master.lastname, maxcoach.year, maxcoach.games,maxcoach.wins,maxcoach.losses,maxcoach.ties
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
	(maxcoach.coachid = master.coachid);