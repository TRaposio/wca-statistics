SELECT *
FROM (
	SELECT 
		a.personId as ID1, 
		a.personName as Name1, 
		b.personId as ID2, 
		b.personName as Name2, 
		COUNT(*) as BFFs
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
	WHERE 
		a.countryID = 'Italy' AND 
		b.countryId = 'Italy'
	GROUP BY 
		a.personId, 
		b.personId
	)t
WHERE 
	t.Name1 < t.Name2
ORDER BY 
	5 DESC
LIMIT 20
