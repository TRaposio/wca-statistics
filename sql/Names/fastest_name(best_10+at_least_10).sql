/* average of top 10 Italians for each name with at least 10 competitors */

SELECT 
	t.name, 
	ROUND(AVG(t.best)/100,2) as avg
FROM (
	SELECT 
		*, 
		RANK() OVER (PARTITION BY q.name ORDER BY q.best ASC) as name_rank
	FROM (
		SELECT 
			r.best, 
			SUBSTRING_INDEX(p.name,' ', 1) as name, 
			r.person_id
		FROM 
			ranks_average r 
		JOIN 
			persons p
		ON 
			r.person_id = p.wca_id
		WHERE 
			r.event_id = '333' AND
            p.country_id = 'Italy'
		)q
	)t
WHERE 
	t.name_rank <= 10
GROUP BY 
	t.name
HAVING 
	COUNT(DISTINCT t.person_id) = 10
ORDER BY 
	avg ASC
