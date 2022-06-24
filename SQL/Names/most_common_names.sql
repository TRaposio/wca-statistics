SELECT 
	t.comp, 
	t.name, 
	COUNT(DISTINCT t.wcaid) as count

FROM(
	SELECT 
		r.competitionId as comp, 
		r.personId as wcaid, 
		SUBSTRING_INDEX(r.personName,' ', 1) as name
	FROM 
		Results r
	WHERE 
		r.countryId = 'Italy'
	GROUP BY 
		r.competitionId, 
		r.personId
	)t

GROUP BY 
	t.comp, 
	t.name
ORDER BY 
	3 DESC
