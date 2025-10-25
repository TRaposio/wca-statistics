/* People with a podium in 2x2 to 7x7 at Italian Championship */

SELECT 
	t.person_id as id, 
	p.name, 
	t.competition_id
FROM (
	SELECT 
		r.person_id, 
		r.competition_id, 
		r.event_id, 
		RANK() OVER (PARTITION BY r.competition_id, r.event_id ORDER BY r.pos ASC) as nr_rank
	FROM 
		results r, 
		championships s
	WHERE 
		s.competition_id = r.competition_id AND 
		s.championship_type = 'IT' AND 
		r.round_type_id IN ('c','f') AND 
		r.best > 0 AND 
		r.country_id = 'Italy' AND
		r.event_id IN ('222', '333', '444', '555', '666', '777')
	)t
JOIN 
	persons p
ON 
	p.wca_id = t.person_id
WHERE
	t.nr_rank <= 3
GROUP BY
	t.person_id, t.competition_id
HAVING
	COUNT(DISTINCT t.event_id) = 6
ORDER BY 
        RIGHT(t.competition_id, 4) ASC
