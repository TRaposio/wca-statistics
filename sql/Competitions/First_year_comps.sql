SELECT 
	r.person_id as wcaid, 
	r.person_name as nome, 
	RIGHT(c.id,4) as anno, 
	COUNT(DISTINCT c.id) as gare_primo_anno
FROM 
	results r, 
	competitions c
WHERE 
	r.competition_id = c.id AND 
	LEFT(r.person_id, 4) = RIGHT(c.id,4)
GROUP BY 
	r.person_id
ORDER BY 
	4 DESC
