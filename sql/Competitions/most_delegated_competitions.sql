SELECT 
	d.delegate_id, 
	u.name, 
	p.country_id, 
	COUNT(DISTINCT d.competition_id) as delegated_comps
FROM 
	competition_delegates d, 
	users u, 
	persons p, 
	competitions c
WHERE 
	d.delegate_id = u.id AND 
	u.wca_id = p.wca_id AND 
	p.country_id = ':country' AND 
	c.id = d.competition_id AND 
	c.cancelled_at IS NULL AND 
	c.results_submitted_at IS NOT NULL
GROUP BY 
	delegate_id
ORDER BY 
	4 DESC
