#Below a faster version with no previous competition

SELECT 
	t.id, 
	t.date, 
	DATEDIFF(t.date, t.prev_date) as days_since_last_comp
FROM (
	SELECT 
		k.id,
		k.start_date as date,
		(SELECT 
			q.end_date 
		FROM 
			competitions q 
		WHERE 
			q.country_id = ':country' AND 
			q.end_date < k.start_date AND 
			q.cancelled_at IS NULL 
		ORDER BY 
			q.end_date DESC 
		LIMIT 1) as prev_date

	FROM 
		competitions k
	WHERE 
		k.country_id = ':country' AND 
		k.cancelled_at IS NULL 
	ORDER BY 
		date DESC
	) t

ORDER BY 
	days_since_last_comp DESC
