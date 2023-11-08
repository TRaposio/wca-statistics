SELECT 
	r.personId, 
	r.personName as name, 
	RIGHT(c.id,4) as year, 
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
	RIGHT(c.id,4)
ORDER BY 
	4 DESC
