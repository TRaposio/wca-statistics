WITH finals
AS (
	SELECT 
		s.competition_id as comp, 
		r.eventId as event, 
		COUNT(DISTINCT r.personId) as persons
	FROM 
		championships s, 
		Results r
	WHERE 
		s.competition_id = r.competitionId AND 
		s.championship_type IN ('_Europe', 'world') AND
		r.roundTypeId IN ('c','f')
	GROUP BY 
		r.competitionId, 
		r.eventId)

SELECT 
	f.comp, 
	f.event, 
	f.persons, 
	COUNT(DISTINCT t.personId) AS Italians, 
	ROUND(COUNT(DISTINCT t.personId) / f.persons ,2) as pct
FROM 
	finals f, 
	Results t
WHERE 
	f.comp = t.competitionId AND
	f.event = t.eventId AND
	t.roundTypeId IN ('c','f') AND
	t.countryId = 'Italy'
GROUP BY 
	t.competitionId, 
	t.eventId
ORDER BY 
	5 DESC