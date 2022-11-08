#ranks competitions by rounds/day

WITH 
rounds AS(
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
		2 DESC),
days AS(
	SELECT 
		c.id, 
		DATEDIFF(c.end_date, c.start_date) + 1 as days 
	FROM 
		Competitions c)

SELECT 
	a.competitionId, 
	b.days, 
	ROUND(a.rounds / b.days, 2) as round_density
FROM 
	rounds a, 
	days b
WHERE 
	a.competitionId = b.id 
	#AND b.days > 1#
ORDER BY 
	3 DESC
