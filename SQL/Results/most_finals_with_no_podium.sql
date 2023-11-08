/* Italians with no medals ranked by number of finals they qualified for (no combined finals) */

SELECT 
	personId as WCAID,
	personName as Name,
	COUNT(DISTINCT(competitionId)) as Competitions
FROM 
	Results
WHERE 
	countryId = 'Italy' AND 
	roundTypeId ='f' AND 
	best > 0
GROUP BY 
	personId
HAVING 
	MIN(pos) > 3
ORDER BY 
	Competitions DESC
