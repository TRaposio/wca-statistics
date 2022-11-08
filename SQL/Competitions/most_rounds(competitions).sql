
#ranks competitions by number of rounds

SELECT 
	t.competitionId, 
	COUNT(*) as rounds
FROM (
	SELECT DISTINCT 
		competitionId, 
		eventId, 
		roundTypeId
	FROM 
		Results)t
GROUP BY 
	t.competitionId
ORDER BY
	2 DESC