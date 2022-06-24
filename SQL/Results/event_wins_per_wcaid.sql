SELECT 
	eventId as Events,
	COUNT(eventId) as Wins
FROM 
	Results
WHERE 
	roundTypeId IN ('f','c') AND 
	pos = 1 AND 
	personId = '2009ROTA01' AND 
	best > 0
GROUP BY 
	Events
ORDER BY 
	Wins DESC
