SELECT 
	r.competition_id as comp, 
	COUNT(DISTINCT SUBSTRING_INDEX(r.person_name,' ', 1)) as DifferentNames, 
	COUNT(DISTINCT r.person_id) as Competitors
FROM 
	results r, 
	competitions c
WHERE 
	r.competition_id = c.id AND 
	c.country_id = 'Italy'
GROUP BY 
	r.competition_id
ORDER BY 2 DESC
