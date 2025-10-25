
#ranks competitions by number of rounds

SELECT 
	t.competition_id, 
	COUNT(*) as rounds
FROM (
	SELECT DISTINCT 
		competition_id, 
		event_id, 
		round_type_id
	FROM 
		results r
	JOIN
		competitions c
	ON
		c.id = r.competition_id
	WHERE
		c.country_id = ':country'
	) t
GROUP BY 
	t.competition_id
ORDER BY
	2 DESC