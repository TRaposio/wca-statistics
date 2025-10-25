/* days since last gold of people that competed after the pandemic (2021) */

SELECT 
	*, 
	DATEDIFF(CURDATE(),t.last_gold) as dslg
FROM (
	SELECT 
		r.person_id, 
		r.person_name,
		MAX(c.end_date) as last_gold
	FROM 
		results r, 
		competitions c
	WHERE 
		c.id = r.competition_id AND 
		r.country_id = ':country' AND
		r.round_type_id IN ('c','f') AND
		r.pos = 1 AND
		r.best > 0 AND
		r.person_id IN (
			SELECT 
				DISTINCT(r2.person_id)
			FROM 
				results r2, 
				competitions c2
			WHERE 
				r2.competition_id = c2.id AND
				r2.country_id = ':country2' AND
				RIGHT(c2.id,4) > 2020
			)
	GROUP BY 
		r.person_id
	) t
ORDER BY 
	dslg DESC
