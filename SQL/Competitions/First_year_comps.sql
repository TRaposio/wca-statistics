SELECT 
	r.personId as wcaid, 
	r.personName as nome, 
	c.year as anno, 
	COUNT(DISTINCT c.id) as gare_primo_anno
FROM 
	Results r, 
	Competitions c
WHERE 
	r.competitionId = c.id AND 
	LEFT(r.personId, 4) = c.year
GROUP BY 
	r.personId
ORDER BY 
	4 DESC