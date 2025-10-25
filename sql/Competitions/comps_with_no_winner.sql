SELECT 
	country_id as Country, 
	COUNT(*) as gts
FROM competitions
WHERE 
	cancelled_at IS NULL AND
	main_event_id IS NULL
GROUP BY country_id
ORDER BY gts DESC
