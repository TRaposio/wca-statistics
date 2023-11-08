/* Italians ranked by single / average ratio in [event] */

SELECT 
	q.WCAID, 
	p.name, 
	q.ratio
FROM 
	(SELECT 
		s.personId as WCAID, 
		s.best/a.best as ratio
	FROM 
		RanksSingle s
	JOIN 
		RanksAverage a
	ON 
		s.personId = a.personId AND 
		s.eventId = a.eventId
	WHERE 
		s.eventId = ':event'
	) as q
JOIN 
	Persons p
ON 
	p.wca_id = q.WCAID
WHERE 
	p.countryId = 'Italy' AND 
	p.subid = 1
ORDER BY 
	3 DESC
