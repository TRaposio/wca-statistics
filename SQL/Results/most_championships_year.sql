SELECT 
	r.personId, 
	r.personName as name, 
	c.year, 
	COUNT(DISTINCT r.competitionId) as championships
FROM 
	Results r, 
	Competitions c,
    championships s
WHERE 
	r.competitionId = c.id AND
    s.competition_id = c.id
GROUP BY 
	r.personid, 
	c.year
ORDER BY 
	4 DESC