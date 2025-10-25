SELECT 
	q.country_id, 
	SUM(IF(dslc<=(2*365),1,0))/COUNT(*) AS recently_competed_pct
FROM (
	SELECT 
		*, 
		DATEDIFF(CURDATE(),t.last_comp) as dslc
	FROM (
		SELECT 
			r.person_id, 
			r.person_name, 
			r.country_id, 
			MAX(c.end_date) as last_comp
		FROM 
			results r, 
			competitions c
		WHERE 
			c.id = r.competition_id
		GROUP BY 
			r.person_id
		)t
	)q
GROUP BY 
	q.country_id
HAVING (
	SELECT 
		COUNT(DISTINCT id) 
	FROM 
		competitions 
	WHERE 
		cancelled_at IS NULL AND 
		country_id = q.country_id
	) >= 5
ORDER BY
	2 DESC
