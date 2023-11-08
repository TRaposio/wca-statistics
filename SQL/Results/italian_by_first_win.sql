/* Italians listed by date of their first gold medal */

SELECT 
	personId, 
	personName, 
	MIN(c.end_date) as first_win
FROM 
	Results r,
        Competitions c 
WHERE 
	r.competitionId = c.id AND
	r.best > 0 AND 
	r.pos=1 AND 
	r.roundTypeId IN ('c','f') AND 
	r.countryId = "Italy"   
GROUP BY 
	r.personId, 
	r.personName 
ORDER BY 
	3 asc;
