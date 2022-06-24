SELECT 
	*, 
	DATEDIFF(CURDATE(),t.last_comp) as dslc
FROM (
	SELECT 
		r.personId, 
		r.personName, 
		MAX(c.end_date) as last_comp
	FROM 
		Results r, 
		Competitions c
	WHERE 
		c.id = r.competitionId AND 
		r.countryId = 'Italy'
	GROUP BY 
		r.personId
	) t
ORDER BY 
	dslc DESC
