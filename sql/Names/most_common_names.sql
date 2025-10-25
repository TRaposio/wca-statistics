SELECT 
	t.comp, 
	t.name, 
	COUNT(DISTINCT t.wcaid) as count

FROM(
	SELECT 
		r.competition_id as comp, 
		r.person_id as wcaid, 
		SUBSTRING_INDEX(r.person_name,' ', 1) as name
	FROM 
		results r
	WHERE 
		r.country_id = 'Italy'
	GROUP BY 
		r.competition_id, 
		r.person_id
	)t

GROUP BY 
	t.comp, 
	t.name
ORDER BY 
	3 DESC
