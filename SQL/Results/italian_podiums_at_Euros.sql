SELECT 
	t.personId as id,
    t.personName as name, 
	t.competitionId,
    t.eventId
FROM (
	SELECT 
		r.personId,
        r.personName, 
		r.competitionId,
        r.countryId,
		r.eventId, 
		RANK() OVER (PARTITION BY r.competitionId, r.eventId ORDER BY r.pos ASC) as nr_rank
	FROM 
		Results r, 
		championships s
	WHERE 
		s.competition_id = r.competitionId AND 
		s.championship_type = '_Europe' AND 
		r.roundTypeId IN ('c','f') AND 
		r.best > 0 AND 
		r.countryId IN (Select id from Countries WHERE continentId = '_Europe')
	)t
WHERE
	t.nr_rank <= 3 AND
    t.countryId = 'Italy'
ORDER BY
    RIGHT(t.competitionId,4) ASC