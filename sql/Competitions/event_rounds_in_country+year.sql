/* Number of rounds held per event in country, year */

SELECT 
	p.Event, 
	SUM(p.TimesHeld) as Rounds 
FROM 
	(SELECT 
		event_id as Event, 
		COUNT(DISTINCT round_type_id) as TimesHeld
	FROM 
		results r
	JOIN 
		competitions c
	ON 
		r.competition_id = c.id
	WHERE 
		c.country_id = ':country' AND 
		RIGHT(c.id,4) = :year
	GROUP BY 
		event_id, 
		competition_id)p
GROUP BY 
	p.Event
ORDER BY 
	Rounds DESC
