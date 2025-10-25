/* Percentage of Italian scorecard per year */

WITH sc AS(
	SELECT 
		RIGHT(competition_id,4) as Year,
		COUNT(*) as Scorecards
	FROM 
		results r,
		competitions c
	WHERE 
		r.competition_id = c.id AND 
		c.country_id = ':country'
	GROUP BY 
		RIGHT(competition_id,4)
)

SELECT 
	sc1.Year, 
	ROUND(sc1.Scorecards / (SELECT SUM(Scorecards) FROM sc),2) as Scorecards_pct
FROM 
	sc as sc1
ORDER BY 
	2 DESC