/* Average of all people that share the same name (at least 5) */

SELECT 
	t.name, 
	ROUND(AVG(t.best)/100,2) as avg, 
	COUNT(DISTINCT t.person_id) as persons
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
)t
GROUP BY 
	t.name
HAVING 
	persons > 4
ORDER BY 
	avg ASC
