WITH comps AS(
	SELECT 
		r.personId as id, 
		COUNT(DISTINCT r.competitionId) as compN
	FROM 
		Results r
	GROUP BY 
		r.personId
)

SELECT 
	c.compN,
	COUNT(DISTINCT c.id) as competitors
FROM 
	comps c, 
	Persons p
WHERE 
	c.id = p.id AND 
	p.gender = 'f'
GROUP BY 
	c.compN
ORDER BY 
	c.compN ASC