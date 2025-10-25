SELECT 
	t.ID ID, 
	t.Name, COUNT(*) as CompWins
FROM (
	SELECT 
		r.person_id as ID, 
		r.person_name as Name, 
		r.competition_id as comp, 
		c.main_event_id
	FROM 
		results r
	JOIN 
		competitions c
	ON 
		r.competition_id = c.id
	WHERE 
		r.round_type_id IN ('f','c') AND 
		r.best > 0 AND 
		r.event_id = c.main_event_id AND 
		r.pos = 1 AND 
		r.country_id = 'Italy'
	)t
GROUP BY 
	t.ID
ORDER BY 
	CompWins DESC
