/* distribution of the number of competitions of female competitors */ 

WITH comps AS(
	SELECT 
		r.personId as wca_id, 
		COUNT(DISTINCT r.competitionId) as compN
	FROM 
		Results r
	GROUP BY 
		r.personId
)
SELECT 
	c.compN,
	COUNT(DISTINCT c.wca_id) as f_competitors
FROM 
	comps c, 
	Persons p
WHERE 
	c.wca_id = p.wca_id AND 
	p.gender = 'f'
GROUP BY 
	c.compN
ORDER BY 
	c.compN ASC
