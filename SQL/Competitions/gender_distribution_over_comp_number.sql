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
	ROUND(SUM(IF(p.gender = 'm',1,0))/COUNT(*),2) as male_pct, 
	ROUND(SUM(IF(p.gender = 'f',1,0))/COUNT(*),2) as female_pct
FROM 
	comps c, 
	Persons p
WHERE 
	c.id = p.id
GROUP BY 
	c.compN
ORDER BY 
	c.compN ASC