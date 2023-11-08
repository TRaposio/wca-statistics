SELECT 
	r.personId, 
	r.personName as name, 
	RIGHT(c.id,4) as year, 
	COUNT(DISTINCT r.competitionId) as comps
FROM 
	Results r, 
	Competitions c
WHERE 
	r.competitionId = c.id AND 
	r.countryId = 'Italy'
GROUP BY 
	r.personId, 
	RIGHT(c.id,4)
ORDER BY 
	4 DESC
