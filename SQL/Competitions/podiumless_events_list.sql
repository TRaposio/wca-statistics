SELECT 
	competitionId as comp, 
	eventId as event
FROM 
	Results
WHERE 
	pos < 4 AND 
	roundTypeId IN ('f','c')
GROUP BY 
	competitionId, 
	event
HAVING 
	MAX(best) < 0
ORDER BY 
	RIGHT(competitionId,4) DESC
