SELECT *
FROM 
	(SELECT 
		p.countryId, 
		r.best as mean, 
		RANK() OVER (PARTITION BY p.countryId ORDER BY r.id ASC) as truerank
	FROM 
		RanksAverage r
	JOIN 
		Persons p
	ON 
		r.personId = p.id
	WHERE 
		r.eventId = '333fm' AND 
		p.subid = 1
	) q
WHERE 
	q.truerank = 10
ORDER BY 
	q.mean ASC
