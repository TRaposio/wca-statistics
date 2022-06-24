SELECT 
	t.name, 
	COUNT(DISTINCT t.id) as Freq
FROM (
	SELECT 
		p.id, 
		SUBSTRING_INDEX(p.name,' ', 1) as name
	FROM 
		Persons p 
	WHERE 
		p.countryId = 'Italy'
	)t
GROUP BY 
	t.name
ORDER BY 
	2 DESC
