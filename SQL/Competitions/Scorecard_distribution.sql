/* Percentage of Italian scorecard per year */

WITH sc AS(
	SELECT 
		RIGHT(competitionId,4) as Year,
		COUNT(*) as Scorecards
	FROM 
		Results r,
		Competitions c
	WHERE 
		r.competitionId = c.id AND 
		c.countryId = 'Italy'
	GROUP BY 
		RIGHT(competitionId,4)
)

SELECT 
	sc1.Year, 
	ROUND(sc1.Scorecards / (SELECT SUM(Scorecards) FROM sc),2) as Scorecards_pct
FROM 
	sc as sc1
ORDER BY 
	2 DESC