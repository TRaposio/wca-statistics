SELECT 
	r.person_id, 
	r.person_name, 
	SUM(IF(r.pos = 1,1,0)) as I°, 
	SUM(IF(r.pos = 2,1,0)) as II°, 
	SUM(IF(r.pos = 3,1,0)) as III° , 
	SUM(IF(r.pos <= 3,1,0)) as Podiums, 
	4*SUM(IF(r.pos = 1,1,0))+2*SUM(IF(r.pos = 2,1,0))+1*SUM(IF(r.pos = 3,1,0)) as Points
FROM 
	competitions c, 
	results r
WHERE 
	c.id = r.competition_id AND  
	r.country_id = ':country' AND 
	r.round_type_id IN ('c','f') AND 
	r.best > 0 AND 
	r.pos < 4 AND 
	RIGHT(c.id,4) = :year
GROUP BY 
	r.person_id
ORDER BY 
	Points DESC, 
	I° DESC, 
	II° DESC, 
	III° DESC
