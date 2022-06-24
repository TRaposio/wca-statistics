SELECT 
	r.personId, 
	r.personName as name, 
	c.year, 
	COUNT(DISTINCT r.competitionId) as comps
FROM 
	Results r, 
	Competitions c
WHERE 
	r.competitionId = c.id AND 
	r.countryId = 'Italy'
GROUP BY 
	r.personid, 
	c.year
ORDER BY 
	4 DESC
