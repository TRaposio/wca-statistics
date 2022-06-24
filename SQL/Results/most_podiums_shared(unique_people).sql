SELECT 
	a.personId as aID, 
	a.personName as Name, 
	COUNT(DISTINCT b.personId) as GTS
FROM (
	SELECT * 
	FROM 
		Results r 
	WHERE 
		r.roundTypeId IN ('f','c') AND 
		r.best > 0 AND 
		r.pos < 4
	)a
JOIN (
	SELECT * 
	FROM 
		Results q 
	WHERE 
		q.roundTypeId IN ('f','c') AND 
		q.best > 0 AND 
		q.pos < 4
	)b
ON 
	a.competitionId = b.competitionId AND 
	a.eventId = b.eventId AND 
	a.personId <> b.personId
GROUP BY 
	a.personId
ORDER BY 
	3 DESC
LIMIT 20
