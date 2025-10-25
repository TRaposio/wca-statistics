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
	COUNT(DISTINCT p.person_id) as newcomers
FROM 
	firstdate f, 
	persone p
WHERE 
	f.person_id = p.person_id AND
	p.start_date = f.data
GROUP BY 
	p.competition_id
ORDER BY 
	2 DESC
