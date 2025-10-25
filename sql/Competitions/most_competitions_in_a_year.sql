SELECT 
	r.person_id, 
	r.person_name as name, 
	RIGHT(c.id,4) as year, 
	COUNT(DISTINCT r.competition_id) as comps
FROM 
	results r, 
	competitions c
WHERE 
	r.competition_id = c.id AND 
	r.country_id = ':country'
GROUP BY 
	r.person_id, 
	RIGHT(c.id,4)
ORDER BY 
	4 DESC
