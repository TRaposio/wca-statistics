SELECT *
FROM (
	SELECT 
		a.person_id as ID1, 
		a.person_name as Name1, 
		b.person_id as ID2, 
		b.person_name as Name2, 
		COUNT(*) as BFFs
	FROM (
		SELECT * 
		FROM 
			results r 
			WHERE 
				r.round_type_id IN ('f','c') AND 
				r.best > 0 AND 
				r.pos < 4
		)a
	JOIN (
		SELECT * 
		FROM 
			results q 
		WHERE 
			q.round_type_id IN ('f','c') AND 
			q.best > 0 AND 
			q.pos < 4
		)b
	ON 
		a.competition_id = b.competition_id AND 
		a.event_id = b.event_id AND 
		a.person_id <> b.person_id
	WHERE 
		a.country_id = 'Italy' AND 
		b.country_id = 'Italy'
	GROUP BY 
		a.person_id, 
		b.person_id
	)t
WHERE 
	t.Name1 < t.Name2
ORDER BY 
	5 DESC
LIMIT 20
