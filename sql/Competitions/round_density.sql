#ranks competitions by rounds/day

WITH 
rounds AS(
	SELECT 
		t.competition_id, 
		COUNT(*) as rounds
	FROM (
		SELECT DISTINCT 
			competition_id, 
			event_id, 
			round_type_id
		FROM 
			results)t
	GROUP BY 
		t.competition_id
	ORDER BY 
		2 DESC),
days AS(
	SELECT 
		c.id, 
		DATEDIFF(c.end_date, c.start_date) + 1 as days 
	FROM 
		competitions c)

SELECT 
	a.competition_id, 
	b.days, 
	ROUND(a.rounds / b.days, 2) as round_density
FROM 
	rounds a, 
	days b
WHERE 
	a.competition_id = b.id 
	#AND b.days > 1#
ORDER BY 
	3 DESC
