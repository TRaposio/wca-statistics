WITH 
firstdate AS(
	SELECT 
		r.personId, 
		MIN(c.start_date) as data
	FROM 
		Results r, 
		Competitions c
	WHERE 
		r.competitionId = c.id 
	GROUP BY
		r.personId
),
persone AS(
	SELECT
		r.personId,
		r.competitionId,
		c.start_date
	FROM
		Results r,
		Competitions c
	WHERE
		r.competitionId = c.id AND
		c.countryId = 'Italy'
	GROUP BY 
		r.personId, 
		c.id
)

SELECT
	p.competitionId as competition, 
	COUNT(DISTINCT p.personId) as newcomers
FROM 
	firstdate f, 
	persone p
WHERE 
	f.personId = p.personId AND
	p.start_date = f.data
GROUP BY 
	p.competitionId
ORDER BY 
	2 DESC
