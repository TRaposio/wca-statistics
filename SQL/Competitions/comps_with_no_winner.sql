SELECT 
	countryId as Country, 
	COUNT(*) as gts
FROM Competitions
WHERE 
	cancelled_at IS NULL AND
	main_event_id IS NULL
GROUP BY countryId
ORDER BY gts DESC
