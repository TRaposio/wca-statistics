SELECT 
	eventId, 
	COUNT(eventId)/17 as GTS
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
	HAVING COUNT(worldRank) = 1) p
GROUP BY 
	eventId
ORDER BY 
	GTS DESC
