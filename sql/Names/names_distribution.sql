SELECT 
	t.name, 
	COUNT(DISTINCT t.wca_id) as Freq
FROM (
	SELECT 
		p.wca_id, 
		SUBSTRING_INDEX(p.name,' ', 1) as name
	FROM 
		persons p 
	WHERE 
		p.country_id = 'Italy'
	)t
GROUP BY 
	t.name
ORDER BY 
	2 DESC
