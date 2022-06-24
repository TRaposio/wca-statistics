SELECT 
	personId, 
	personName, 
	competitionId, 
	eventId, 
	CONCAT(c.year,'-',LPAD(c.month,2,0),'-',LPAD(c.day,2,0)) d 
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
	r.CountryId="Italy" 
ORDER BY 
	d asc;
