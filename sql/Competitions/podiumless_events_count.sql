SELECT 
	t.event as event, 
	COUNT(*) as count
FROM (
	SELECT 
		competition_id as comp, 
		event_id as event
	FROM results
	WHERE pos < 4 AND round_type_id IN ('f','c')
	GROUP BY 
		competition_id, 
		event
	HAVING 
		MAX(best) < 0
	ORDER BY 
		RIGHT(competition_id,4) DESC
	)t
GROUP BY 
	t.event
ORDER BY 2 DESC
