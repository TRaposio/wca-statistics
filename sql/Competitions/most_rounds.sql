# Ranks competitors by their cumulative number of rounds #

SELECT 
	p.person_id as Id, 
	person_name as Name, 
	SUM(p.TimesHeld) as Rounds 
FROM 
	(SELECT 
		person_id, 
		person_name, 
		COUNT(DISTINCT round_type_id) as TimesHeld
	FROM 
		results r
	JOIN 
		competitions c
	ON 
		r.competition_id = c.id
	WHERE 
		c.country_id = ':country'
	GROUP BY 
		person_id, 
		competition_id) p
GROUP BY 
	p.person_id
ORDER BY 
	Rounds DESC
