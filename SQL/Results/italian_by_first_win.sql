SELECT 
	personId, 
	personName, 
	MIN(CONCAT(c.year,'-',LPAD(c.month,2,0),'-',LPAD(c.day,2,0))) 
FROM 
	Results r 
JOIN 
	Competitions c 
ON 
	c.id=r.competitionId 
WHERE 
	r.best > 0 AND 
	r.pos=1 AND 
	r.roundTypeId IN ('c','f') AND 
	r.countryId="Italy"  
GROUP BY 
	r.personId, 
	r.personName 
ORDER BY 
	3 asc;
