SELECT 
	t.person_id as person,
	t.best as Avg
FROM ranks_average as t
WHERE 
	t.event_id = '333oh' AND 
	t.best < (
		SELECT r.best
		FROM ranks_average as r
		WHERE 
			r.event_id = '333' AND 
			r.person_id = t.person_id)
