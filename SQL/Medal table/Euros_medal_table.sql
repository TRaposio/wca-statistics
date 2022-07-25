SELECT  
	t.countryId as country, 
	SUM(IF(t.nr_rank = 1,1,0)) as I°, 
	SUM(IF(t.nr_rank = 2,1,0)) as II°, 
	SUM(IF(t.nr_rank = 3,1,0)) as III° , 
	SUM(IF(t.nr_rank <= 3,1,0)) as Podiums
FROM (
	SELECT 
		r.personId, 
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
GROUP BY 
	t.countryId
ORDER BY 
	I° DESC, 
	II° DESC, 
	III° DESC
