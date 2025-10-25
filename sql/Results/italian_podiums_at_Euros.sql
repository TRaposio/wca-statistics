/* list of Italian podium finishers at the Rubik's Cube European Championship */

SELECT 
	t.person_id as id,
    t.person_name as name,
	t.competition_id as competition,
    t.event_id as event,
    t.er_rank as position
FROM (
	SELECT 
		r.person_id,
        r.person_name,
		r.competition_id,
        r.country_id,
		r.event_id,
		RANK() OVER (PARTITION BY r.competition_id, r.event_id ORDER BY r.pos ASC) as er_rank
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
WHERE
	t.er_rank <= 3 AND
	t.country_id = 'Italy'
ORDER BY
    RIGHT(t.competition_id,4) ASC
