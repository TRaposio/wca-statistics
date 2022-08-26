SELECT 
	*, 
	DATEDIFF(CURDATE(),t.last_gold) as dslg
FROM (
	SELECT 
		r.personId, 
		r.personName,
		MAX(c.end_date) as last_gold
	FROM 
		Results r, 
		Competitions c
	WHERE 
		c.id = r.competitionId AND 
		r.countryId = 'Italy' AND
		r.roundTypeId IN ('c','f') AND
		r.pos = 1 AND
		r.best > 0 AND
		r.personId IN (
			SELECT 
				DISTINCT(r2.personId)
			FROM 
				Results r2, 
				Competitions c2
			WHERE 
				r2.competitionId = c2.id AND
				r2.countryId = 'Italy' AND
				c2.year > 2021
			)
	GROUP BY 
		r.personId
	) t
ORDER BY 
	dslg DESC
