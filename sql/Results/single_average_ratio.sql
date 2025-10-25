/* Italians ranked by single / average ratio in [event] */

SELECT 
	q.wcaid, 
	p.name, 
	q.ratio
FROM 
	(SELECT 
		s.person_id as wcaid, 
		s.best/a.best as ratio
	FROM 
		ranks_single s
	JOIN 
		ranks_average a
	ON 
		s.person_id = a.person_id AND 
		s.event_id = a.event_id
	WHERE 
		s.event_id = ':event'
	) as q
JOIN 
	persons p
ON 
	p.wca_id = q.wcaid
WHERE 
	p.country_id = 'Italy' AND 
	p.sub_id = 1
ORDER BY 
	3 DESC
