SELECT 
	t.personId as person,
	t.best as Avg
FROM RanksAverage as t
WHERE 
	t.eventId = '333oh' AND 
	t.best < (
		SELECT r.best
		FROM RanksAverage as r
		WHERE 
			r.eventId = '333' AND 
			r.personId = t.personId)
