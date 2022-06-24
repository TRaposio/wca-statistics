SELECT 
	t.personId as id, 
	p.name, 
	p.countryId as country, 
	SUM(IF(t.nr_rank = 1,1,0)) as I°, 
	SUM(IF(t.nr_rank = 2,1,0)) as II°, 
	SUM(IF(t.nr_rank = 3,1,0)) as III° , 
	SUM(IF(t.nr_rank <= 3,1,0)) as Podiums
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
		r.countryId = 'Italy'
	)t
JOIN 
	Persons p
ON 
	p.id = t.personId
GROUP BY 
	t.personId
ORDER BY 
	I° DESC, 
	II° DESC, 
	III° DESC
