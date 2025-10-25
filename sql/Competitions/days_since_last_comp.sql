SELECT 
	*, 
	DATEDIFF(CURDATE(),t.last_comp) as dslc
FROM (
	SELECT 
		r.person_id, 
		r.person_name, 
		MAX(c.end_date) as last_comp
	FROM 
		results r, 
		competitions c
	WHERE 
		c.id = r.competition_id AND 
		r.country_id = ':country'
	GROUP BY 
		r.person_id
	) t
ORDER BY 
	dslc DESC
