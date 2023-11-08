SELECT 
	p.Event, 
	SUM(p.TimesHeld) as Rounds 
FROM 
	(SELECT 
		eventId as Event, 
		COUNT(DISTINCT roundTypeId) as TimesHeld
	FROM 
		Results r
	JOIN 
		Competitions c
	ON 
		r.competitionId = c.id
	WHERE 
		r.personId = ':wca_id' AND 
		RIGHT(c.id,4) = :year
	GROUP BY 
		eventId, 
		competitionId)p
GROUP BY 
	p.Event
ORDER BY 
	Rounds DESC
