/* Competitions in which the same 3 people shared all podiums, ordered by the number of events (at least 2 events) */

SELECT 
	competitionId as competition,
    	COUNT(DISTINCT eventId) as podiums,
    	GROUP_CONCAT(DISTINCT(personId) SEPARATOR ', ') as IdList,
    	GROUP_CONCAT(DISTINCT(personName) SEPARATOR ', ') as names
FROM 
	Results 
WHERE 
	roundTypeId IN ("f", "c") AND 
	pos <= 3 AND 
	best >0 
GROUP BY 
	competitionId 
HAVING 
	COUNT(DISTINCT eventId) >1 AND 
	COUNT(DISTINCT personId) <=3
ORDER BY
    	podiums DESC
