SELECT
	teams2.name,base2.firstname, base2.lastname, base2.year,base2.points, base2.goal, base2.assist
FROM
	(SELECT
		m.firstname, m.lastname,base.pid, base.tid, base.points, base.goal, base.assist, base.year as year
	FROM
		master m
	JOIN
		(SELECT
			s.playerid as pid, s.tmid as tid, s.pts as points, s.g as goal, s.a as assist, s.year
		FROM
			scoring s
		JOIN
			(SELECT tmid,  MAX(pts) as pts
				FROM
					scoring
				GROUP by (tmid)) maxpts
		ON (s.tmid = maxpts.tmid and s.pts = maxpts.pts)) base
	ON(m.playerid = base.pid)
	) base2
JOIN
	(SELECT
	DISTINCT tmid, name 
	from teams) teams2
ON (base2.tid = teams2.tmid)
;