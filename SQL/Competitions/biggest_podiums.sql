SELECT 
	competitionId as comp, 
	eventId as event, 
	COUNT(*) as PodiumSize
FROM 
	Results
WHERE 
	best > 0 AND 
	pos < 4 AND 
	roundTypeId IN ('f','c')
GROUP BY 
	competitionId, 
	event
ORDER BY 
	3 DESC
LIMIT 20
