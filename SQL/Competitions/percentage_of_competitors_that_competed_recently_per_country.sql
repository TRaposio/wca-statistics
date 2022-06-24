SELECT 
	q.countryId, 
	SUM(IF(dslc<=(2*365),1,0))/COUNT(*) AS recently_competed_pct
FROM (
	SELECT 
		*, 
		DATEDIFF(CURDATE(),t.last_comp) as dslc
	FROM (
		SELECT 
			r.personId, 
			r.personName, 
			r.countryId, 
			MAX(c.end_date) as last_comp
		FROM 
			Results r, 
			Competitions c
		WHERE 
			c.id = r.competitionId
		GROUP BY 
			r.personId
		)t
	)q
GROUP BY 
	q.countryid
HAVING (
	SELECT 
		COUNT(DISTINCT id) 
	FROM 
		Competitions 
	WHERE 
		cancelled_at IS NULL AND 
		countryId = q.countryId
	) >= 5
ORDER BY
	2 DESC
