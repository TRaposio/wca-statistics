# Number of times name x was the most common name at a competition

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
				r.competition_id as comp, 
				r.person_id as wcaid, 
				SUBSTRING_INDEX(r.person_name,' ', 1) as name
			FROM 
				results r, 
				competitions c
			WHERE 
				r.competition_id = c.id AND 
				c.country_id = 'Italy'
			GROUP BY 
				r.competition_id, 
				r.person_id
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
