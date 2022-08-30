SELECT 
	eventId, 
	ROUND(COUNT(eventId)/100,2) as GTS
FROM (
	SELECT 
		personId as id, 
		eventId
	FROM 
		RanksAverage
	WHERE 
		worldRank <= 100 AND 
		eventId NOT IN ('magic','mmagic','333ft','333mbo')
	GROUP BY personId
	HAVING COUNT(eventId) = 1) p
GROUP BY 
	eventId
ORDER BY 
	GTS DESC
