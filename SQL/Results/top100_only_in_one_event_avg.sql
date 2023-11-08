/* Events ranked by percentage of the top 100 that are top 100 only in that event */

SELECT 
	eventId, 
	ROUND(COUNT(eventId)/100,2) as onlytop100_pct
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
	2 DESC
