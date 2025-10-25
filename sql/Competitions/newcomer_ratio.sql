WITH 
firstdate AS(
	SELECT 
		r.person_id, 
		MIN(c.start_date) as data
	FROM 
		results r, 
		competitions c
	WHERE
		r.competition_id = c.id 
	GROUP BY 
		r.person_id
),
persone AS(
	SELECT 
		r.person_id, 
		r.competition_id, 
		c.start_date
	FROM 
		results r, 
		competitions c
	WHERE 
		r.competition_id = c.id AND 
		c.country_id = ':country'
	GROUP BY 
	r.person_id,
	c.id
)

SELECT 
	p.competition_id as competition, 
	SUM(IF(p.start_date = f.data, 1, 0)) as newcomers,
	COUNT(DISTINCT p.person_id) competitors, 
	ROUND(SUM(IF(p.start_date = f.data, 1, 0))/COUNT(DISTINCT p.person_id),2) as ratio
FROM 
	firstdate f, 
	persone p
WHERE 
	f.person_id = p.person_id
GROUP BY 
	p.competition_id
ORDER BY 
	4 DESC



