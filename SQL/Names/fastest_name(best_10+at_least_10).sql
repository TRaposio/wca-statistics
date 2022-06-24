SELECT 
	t.name, 
	ROUND(AVG(t.best)/100,2) as avg
FROM (
	SELECT 
		*, 
		RANK() OVER (PARTITION BY q.name ORDER BY q.best ASC) as name_rank
	FROM (
		SELECT 
			r.best, 
			SUBSTRING_INDEX(p.name,' ', 1) as name, 
			r.personId
		FROM 
			RanksAverage r 
		JOIN 
			Persons p
		ON 
			r.personid = p.id
		WHERE 
			r.eventId = '333'
		)q
	)t
WHERE 
	t.name_rank < 12
GROUP BY 
	t.name
HAVING 
	COUNT(DISTINCT t.personId) = 11
ORDER BY 
	avg ASC
