# Number of times name x was the most common name at a competition (no ties for 1st)

SELECT *
FROM (
	SELECT *, 
	rank() OVER (PARTITION BY q.comp ORDER BY q.count DESC) as name_rank
	FROM (
		SELECT 
			t.comp, 
			t.name, 
			COUNT(DISTINCT t.wcaid) as count
		FROM (
			SELECT 
				r.competitionId as comp, 
				r.personId as wcaid, 
				SUBSTRING_INDEX(r.personName,' ', 1) as name
			FROM 
				Results r
			WHERE 
				r.countryId = 'Italy'
			GROUP BY 
				r.competitionId, 
				r.personId
			)t
		GROUP BY 
			t.comp, 
			t.name
		)q
	)w
WHERE 
	w.name_rank = 1
ORDER BY 
	w.count DESC
