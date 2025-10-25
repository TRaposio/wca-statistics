WITH finals
AS (
	SELECT 
		s.competition_id as comp, 
		r.event_id as event, 
		COUNT(DISTINCT r.person_id) as persons
	FROM 
		championships s, 
		results r
	WHERE 
		s.competition_id = r.competition_id AND 
		s.championship_type IN ('_Europe', 'world') AND
		r.round_type_id IN ('c','f')
	GROUP BY 
		r.competition_id, 
		r.event_id)

SELECT 
	f.comp, 
	f.event, 
	f.persons, 
	COUNT(DISTINCT t.person_id) AS Italians, 
	ROUND(COUNT(DISTINCT t.person_id) / f.persons ,2) as pct
FROM 
	finals f, 
	results t
WHERE 
	f.comp = t.competition_id AND
	f.event = t.event_id AND
	t.round_type_id IN ('c','f') AND
	t.country_id = 'Italy'
GROUP BY 
	t.competition_id, 
	t.event_id
ORDER BY 
	5 DESC