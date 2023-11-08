SELECT 
	r.personId as wcaid, 
	r.personName as nome, 
	RIGHT(c.id,4) as anno, 
	COUNT(DISTINCT c.id) as gare_primo_anno
FROM 
	Results r, 
	Competitions c
WHERE 
	r.competitionId = c.id AND 
	LEFT(r.personId, 4) = RIGHT(c.id,4)
GROUP BY 
	r.personId
ORDER BY 
	4 DESC
