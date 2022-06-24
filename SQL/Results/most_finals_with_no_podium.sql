SELECT 
	personId as WCAID,
	personName as Name,
	COUNT(DISTINCT(competitionId)) as Competitions
FROM 
	Results
WHERE 
	countryId = 'Italy' AND 
	roundTypeId IN ('f','c') AND 
	best > 0
GROUP BY 
	personId
HAVING 
	MIN(pos) > 3
ORDER BY 
	Competitions DESC
