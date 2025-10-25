SELECT
	r.competition_id as championship,
	COUNT(DISTINCT r.country_id ) as nationalities
FROM 
	championships s, 
	results r
WHERE 
	s.competition_id = r.competition_id AND 
	s.championship_type IN ('_Africa', '_Asia', '_Europe', '_North America', '_South America', '_Oceania', 'world') 
GROUP BY 
	s.competition_id
ORDER BY 
	2 DESC