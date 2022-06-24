SELECT 
	q.WCAID, 
	p.name, 
	q.GTS
FROM 
	(SELECT 
			s.personId as WCAID, 
			s.best/a.best as GTS
	FROM 
		RanksSingle s
	JOIN 
			RanksAverage a
	ON 
		s.personId = a.personId AND 
		s.eventId = a.eventId
	WHERE 
		s.eventId = '333'
	) as q
JOIN 
	Persons p
ON 
	p.id = q.WCAID
WHERE 
	p.countryId = 'Italy' AND 
	p.subid = 1
ORDER BY 
	GTS DESC
