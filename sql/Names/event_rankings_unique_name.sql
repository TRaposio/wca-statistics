/* 333 rankings considering only Italians that don't share their name with other competitors */

SELECT 
	t.name,
	t.person_id,
	ROUND(AVG(t.best)/100,2) as avg, 
	COUNT(DISTINCT t.person_id) as persons
FROM(
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
	persons = 1
ORDER BY 
	avg ASC
