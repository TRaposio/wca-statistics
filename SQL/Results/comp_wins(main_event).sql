SELECT 
	t.ID ID, 
	t.Name, COUNT(*) as CompWins
FROM (
	SELECT 
		r.personId as ID, 
		r.personName as Name, 
		r.competitionId as comp, 
		c.main_event_id
	FROM 
		Results r
	JOIN 
		Competitions c
	ON 
		r.competitionId = c.id
	WHERE 
		r.roundTypeId IN ('f','c') AND 
		r.best > 0 AND 
		r.eventId = c.main_event_id AND 
		r.pos = 1 AND 
		r.countryId = 'Italy'
	)t
GROUP BY 
	t.ID
ORDER BY 
	CompWins DESC
