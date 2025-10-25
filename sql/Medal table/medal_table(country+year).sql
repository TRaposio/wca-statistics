SELECT 
	r.person_id, 
	r.person_name, 
	SUM(IF(r.pos = 1,1,0)) as I°, 
	SUM(IF(r.pos = 2,1,0)) as II°, 
	SUM(IF(r.pos = 3,1,0)) as III° , 
	SUM(IF(r.pos <= 3,1,0)) as Podiums
FROM 
	competitions c, 
	results r
WHERE 
	c.id = r.competition_id AND 
	r.country_id = ':country' AND 
	round_type_id IN ('c','f') AND 
	best > 0 AND 
	pos < 4 AND 
	RIGHT(c.id, 4) = :year
GROUP BY 
	r.person_id
ORDER BY 
	I° DESC, 
	II° DESC, 
	III° DESC
