/* distribution of the number of competitions of female competitors */ 

WITH comps AS(
	SELECT 
		r.person_id as wca_id, 
		COUNT(DISTINCT r.competition_id) as compN
	FROM 
		results r
	GROUP BY 
		r.person_id
)
SELECT 
	c.compN,
	COUNT(DISTINCT c.wca_id) as f_competitors
FROM 
	comps c, 
	persons p
WHERE 
	c.wca_id = p.wca_id AND 
	p.gender = 'f'
GROUP BY 
	c.compN
ORDER BY 
	c.compN ASC
