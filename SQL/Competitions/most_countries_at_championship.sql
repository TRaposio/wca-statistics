SELECT
	r.competitionId as championship,
	COUNT(DISTINCT r.CountryId ) as nationalities
FROM 
	championships s, 
	Results r
WHERE 
	s.competition_id = r.competitionId AND 
	s.championship_type IN ('_Africa', '_Asia', '_Europe', '_North America', '_South America', '_Oceania', 'world') 
GROUP BY 
	s.competition_id
ORDER BY 
	2 DESC