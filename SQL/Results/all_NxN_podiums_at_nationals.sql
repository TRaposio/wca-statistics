SELECT 
	t.personId as id, 
	p.name, 
	t.competitionId
FROM (
	SELECT 
		r.personId, 
		r.competitionId, 
		r.eventId, 
		RANK() OVER (PARTITION BY r.competitionId, r.eventId ORDER BY r.pos ASC) as nr_rank
	FROM 
		Results r, 
		championships s
	WHERE 
		s.competition_id = r.competitionId AND 
		s.championship_type = 'IT' AND 
		r.roundTypeId IN ('c','f') AND 
		r.best > 0 AND 
		r.countryId = 'Italy' AND
		r.eventId IN ('222', '333', '444', '555', '666', '777')
	)t
JOIN 
	Persons p
ON 
	p.id = t.personId
WHERE
	t.nr_rank <= 3
GROUP BY
	t.personId, t.competitionId
HAVING
	COUNT(DISTINCT t.eventId) = 6
ORDER BY 
        RIGHT(t.competitionId, 4) ASC
