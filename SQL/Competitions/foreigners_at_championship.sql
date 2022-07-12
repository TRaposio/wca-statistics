SELECT 
	r.competitionId as championship, 
	COUNT(DISTINCT r.personId) as Foreigners, 
	COUNT(DISTINCT r.personId) / (SELECT COUNT(DISTINCT q.personId) FROM Results q WHERE q.competitionId = r.competitionId) as pct
FROM 
	championships s, 
	Results r
WHERE 
	s.competition_id = r.competitionId AND 
	s.championship_type IN ('_Africa', '_Asia', '_Europe', '_North America', '_South America', '_Oceania', 'world') AND 
	r.countryId <> (SELECT k.countryId FROM Competitions k WHERE k.id = s.competition_id)
GROUP BY 
	s.competition_id
ORDER BY 
	3 DESC