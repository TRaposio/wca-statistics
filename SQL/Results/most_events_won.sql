SELECT 
	t.personId as WCAID,
	t.personName as Name,
	COUNT(DISTINCT(t.eventId)) as EventsWon,
	GROUP_CONCAT(DISTINCT(t.eventId) SEPARATOR ', ') as EventList
FROM 
	Results t
WHERE 
	(t.roundTypeId IN ('f','c') AND 
	t.CountryId = 'Italy' AND 
	t.pos = 1AND t.best > 0
GROUP BY 
	t.personId
ORDER BY 
	3 DESC
