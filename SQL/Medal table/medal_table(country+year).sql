SELECT 
	r.personId, 
	p.name, 
	SUM(IF(r.pos = 1,1,0)) as I°, 
	SUM(IF(r.pos = 2,1,0)) as II°, 
	SUM(IF(r.pos = 3,1,0)) as III° , 
	SUM(IF(r.pos <= 3,1,0)) as Podiums
FROM 
	Competitions c, 
	Results r, 
	Persons p
WHERE 
	c.id = r.competitionId AND 
	r.personId = p.id AND 
	r.countryId = 'Italy' AND 
	roundTypeId IN ('c','f') AND 
	best > 0 AND 
	pos < 4 AND 
	c.year = 2022 
GROUP BY 
	r.personId
ORDER BY 
	I° DESC, 
	II° DESC, 
	III° DESC
