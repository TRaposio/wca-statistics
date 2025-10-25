SELECT 
	r.competition_id as championship, 
	COUNT(DISTINCT r.person_id) as Foreigners, 
	COUNT(DISTINCT r.person_id) / (SELECT COUNT(DISTINCT q.person_id) FROM results q WHERE q.competition_id = r.competition_id) as pct
FROM 
	championships s, 
	results r
WHERE 
	s.competition_id = r.competition_id AND 
	s.championship_type IN ('_Africa', '_Asia', '_Europe', '_North America', '_South America', '_Oceania', 'world') AND 
	r.country_id <> (SELECT k.country_id FROM competitions k WHERE k.id = s.competition_id)
GROUP BY 
	s.competition_id
ORDER BY 
	3 DESC