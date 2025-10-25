SELECT  
	t.country_id as country, 
	SUM(IF(t.nr_rank = 1,1,0)) as I°, 
	SUM(IF(t.nr_rank = 2,1,0)) as II°, 
	SUM(IF(t.nr_rank = 3,1,0)) as III° , 
	SUM(IF(t.nr_rank <= 3,1,0)) as Podiums
FROM (
	SELECT 
		r.person_id, 
		r.competition_id,
        r.country_id, 
		r.event_id, 
		RANK() OVER (PARTITION BY r.competition_id, r.event_id ORDER BY r.pos ASC) as nr_rank
	FROM 
		results r, 
		championships s
	WHERE 
		s.competition_id = r.competition_id AND 
		s.championship_type = '_Europe' AND 
		r.round_type_id IN ('c','f') AND 
		r.best > 0 AND
		r.country_id IN (Select id from countries WHERE continent_id = '_Europe')
	)t
GROUP BY 
	t.country_id
ORDER BY 
	I° DESC, 
	II° DESC, 
	III° DESC
