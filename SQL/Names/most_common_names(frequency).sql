SELECT 
	w.name, 
	COUNT(*) as freq
FROM (
	SELECT 
		*, 
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
				Results r, 
				Competitions c
			WHERE 
				r.competitionId = c.id AND 
				c.countryId = 'Italy'
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
GROUP BY 
	w.name
ORDER BY 
	freq DESC
