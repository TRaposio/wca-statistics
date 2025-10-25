SELECT 
	event_id as Events,
	COUNT(event_id) as Wins
FROM 
	results
WHERE 
	round_type_id IN ('f','c') AND 
	pos < 4 AND 
	person_id = ':wcaid' AND 
	best > 0
GROUP BY 
	Events
ORDER BY 
	Wins DESC
