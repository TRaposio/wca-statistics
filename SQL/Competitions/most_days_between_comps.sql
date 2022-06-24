SELECT 
	t.id, 
	t.date, 
	t.prev_comp, 
	t.prev_date, 
	DATEDIFF(t.date, t.prev_date) as days

FROM (
	SELECT 
		k.id,
		k.start_date as date,
		(SELECT 
			q.end_date 
		FROM Competitions q 
		WHERE 
			q.countryId = 'Italy' AND 
			q.end_date < k.start_date AND 
			q.cancelled_at IS NULL 
		ORDER BY 
			q.end_date 
		DESC LIMIT 1) as prev_date,
		(SELECT 
			c.id 
		FROM 
			Competitions c 
		WHERE 
			c.countryId = 'Italy' AND 
			c.end_date < k.start_date AND 
			c.cancelled_at IS NULL 
		ORDER BY 
			c.end_date DESC 
		LIMIT 1) as prev_comp

	FROM 
		Competitions k
	WHERE 
		k.countryId = 'Italy' AND 
		k.cancelled_at IS NULL
	ORDER BY 
		date DESC
	) t

ORDER BY 
	days ASC

#This works only for countries with one competition per weekend
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
			Competitions q 
		WHERE 
			q.countryId = 'Italy' AND 
			q.end_date < k.start_date AND 
			q.cancelled_at IS NULL 
		ORDER BY 
			q.end_date DESC 
		LIMIT 1) as prev_date

	FROM 
		Competitions k
	WHERE 
		k.countryId = 'Italy' AND 
		k.cancelled_at IS NULL AND 
	ORDER BY 
		date DESC
	) t

ORDER BY 
	days_since_last_comp DESC
