SELECT 
	r.competitionId as comp, 
	COUNT(DISTINCT SUBSTRING_INDEX(r.personName,' ', 1)) as DifferentNames, 
	COUNT(DISTINCT r.personId) as Competitors
FROM 
	Results r, 
	Competitions c
WHERE 
	r.competitionId = c.id AND 
	c.countryId = 'Italy'
GROUP BY 
	r.competitionId
ORDER BY 2 DESC
