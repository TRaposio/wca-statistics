SELECT 
	competition_id as comp, 
	event_id as event, 
	COUNT(*) as PodiumSize
FROM 
	results
WHERE 
	best > 0 AND 
	pos < 4 AND 
	round_type_id IN ('f','c')
GROUP BY 
	competition_id, 
	event
ORDER BY 
	3 DESC
LIMIT 20
