SELECT 
	r.personId, 
	r.personName, 
	SUM(IF(r.pos = 1,1,0)) as I°, 
	SUM(IF(r.pos = 2,1,0)) as II°, 
	SUM(IF(r.pos = 3,1,0)) as III° , 
	SUM(IF(r.pos <= 3,1,0)) as Podiums, 
	4*SUM(IF(r.pos = 1,1,0))+2*SUM(IF(r.pos = 2,1,0))+1*SUM(IF(r.pos = 3,1,0)) as Points
FROM 
	Competitions c, 
	Results r
WHERE 
	c.id = r.competitionId AND  
	r.countryId = 'Italy' AND 
	r.roundTypeId IN ('c','f') AND 
	r.best > 0 AND 
	r.pos < 4 AND 
	RIGHT(c.id,4) = 2023 
GROUP BY 
	r.personId
ORDER BY 
	Points DESC, 
	I° DESC, 
	II° DESC, 
	III° DESC
