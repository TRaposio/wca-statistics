SELECT 
	personId, 
	personName, 
	competitionId, 
	eventId, 
	c.end_date as d
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
