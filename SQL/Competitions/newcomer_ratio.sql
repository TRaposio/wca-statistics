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
	SUM(IF(p.start_date = f.data, 1, 0)) as newcomers,
	COUNT(DISTINCT p.personId) competitors, 
	ROUND(SUM(IF(p.start_date = f.data, 1, 0))/COUNT(DISTINCT p.personId),2) as ratio
FROM 
	firstdate f, 
	persone p
WHERE 
	f.personId = p.personId
GROUP BY 
	p.competitionId
ORDER BY 
	4 DESC



