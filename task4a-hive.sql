
SELECT spts.tmid, spts.pts, goalsAndAssists.goal, goalsAndAssists.assists
FROM
	(SELECT tmid, sum(pts) as pts
	FROM
		teams
	GROUP BY (tmid)
	) spts
JOIN
	(SELECT
		tmid, sum(g) as goal , sum(a) as assists
	FROM
		scoring
	GROUP by (tmid)) goalsAndAssists
ON (spts.tmid = goalsAndAssists.tmid);