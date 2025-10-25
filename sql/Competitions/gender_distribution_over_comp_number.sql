/* percentage of male and female competitors for each number of competitions */

WITH comps AS(
	SELECT 
		r.person_id as id, 
		COUNT(DISTINCT r.competition_id) as compN
FROM 
	results r
GROUP BY 
	r.person_id
)

SELECT 
	c.compN, 
	ROUND(SUM(IF(p.gender = 'm',1,0))/COUNT(*),2) as male_pct, 
	ROUND(SUM(IF(p.gender = 'f',1,0))/COUNT(*),2) as female_pct
FROM 
	comps c, 
	persons p
WHERE 
	c.id = p.wca_id
GROUP BY 
	c.compN
ORDER BY 
	c.compN ASC
