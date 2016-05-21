SELECT award_table.award, award_table.year, pts_sum.pts_sum
FROM
	(SELECT ap.playerid as playerid, ap.award as award, ap.year as year
	FROM awardsplayers ap
	JOIN 
		(SELECT  master.playerid as playerid
		FROM master
		JOIN
			(select 
				counts.awards as awardcount, master.firstname as fname, master.lastname as lname
			from 
				(select 
					count(award) as awards, playerid
					from awardsplayers 
					group by playerid) as counts
			JOIN 
				(select max(counts.awards) as maxawards
					from
						(select 
							count(x.award) as awards, x.playerid as playerid
							from 
								(SELECT a.award as award, a.playerid as playerid 
								FROM
									awardsplayers a 
								JOIN
									(SELECT 
										DISTINCT m.playerid, m.firstname, m.lastname
									FROM scoring s
									JOIN (SELECT max(gwg) as gwg FROM scoring ) max
									ON (s.gwg = max.gwg) 
									JOIN master m
									ON (s.playerid = m.playerid)) k
								ON (a.playerid = k.playerid)) x 
							group by x.playerid) as counts) as max
			ON (counts.awards = max.maxawards)
			JOIN 
				master
			ON (master.playerid = counts.playerid)) player
		ON (master.firstname = player.fname and master.lastname = player.lname)) id
	ON (ap.playerid = id.playerid)) award_table
JOIN
	(SELECT SUM(pts_id.pts) as pts_sum, pts_id.year as year-- point sum, year
	FROM 

		(SELECT scoring.pts, scoring.year, scoring.playerid
		FROM scoring
		JOIN 
			(SELECT  master.playerid as playerid
			FROM master
			JOIN
				(select 
					counts.awards as awardcount, master.firstname as fname, master.lastname as lname
				from 
					(select 
						count(award) as awards, playerid
						from awardsplayers 
						group by playerid) as counts
				JOIN 
					(select max(counts.awards) as maxawards
						from
							(select 
								count(x.award) as awards, x.playerid as playerid
								from 
									(SELECT a.award as award, a.playerid as playerid 
									FROM
										awardsplayers a 
									JOIN
										(SELECT 
											DISTINCT m.playerid, m.firstname, m.lastname
										FROM scoring s
										JOIN (SELECT max(gwg) as gwg FROM scoring ) max
										ON (s.gwg = max.gwg) 
										JOIN master m
										ON (s.playerid = m.playerid)) k
									ON (a.playerid = k.playerid)) x 
								group by x.playerid) as counts) as max
				ON (counts.awards = max.maxawards)
				JOIN 
					master
				ON (master.playerid = counts.playerid)) player
			ON (master.firstname = player.fname and master.lastname = player.lname)) id
		ON(scoring.playerid = id.playerid)) pts_id
	GROUP BY pts_id.year) pts_sum
ON (award_table.year = pts_sum.year)
;

