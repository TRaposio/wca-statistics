SELECT 
	person_id, 
	person_name, 
	competition_id, 
	event_id, 
	c.end_date as d
FROM 
	results r 
JOIN 
	competitions c 
ON 
	c.id=r.competition_id 
WHERE 
	r.best > 0 AND 
	r.pos=1 AND 
	r.round_type_id IN ('c','f') AND 
	r.country_id="Italy" 
ORDER BY 
	d asc;
