/* Italians listed by date of their first gold medal */

SELECT 
	person_id, 
	person_name, 
	MIN(c.end_date) as first_win
FROM 
	results r,
    competitions c 
WHERE 
	r.competition_id = c.id AND
	r.best > 0 AND 
	r.pos=1 AND 
	r.round_type_id IN ('c','f') AND 
	r.country_id = "Italy"   
GROUP BY 
	r.person_id, 
	r.person_name 
ORDER BY 
	3 asc;
