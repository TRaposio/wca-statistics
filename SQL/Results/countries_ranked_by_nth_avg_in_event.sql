SELECT 
        q.countryId, 
        wca_statistics_time_format(q.mean, q.eventId, 'average') as Nth_average
FROM 
	(SELECT 
		p.countryId, 
		r.best as mean,
                r.eventId, 
		RANK() OVER (PARTITION BY p.countryId ORDER BY r.worldRank, r.personId ASC) as truerank
	FROM 
		RanksAverage r, Persons p
	WHERE 
		r.personId = p.wca_id AND 
        	r.eventId = ':event' AND
		p.subid = 1
	) q
WHERE 
	q.truerank = :N
ORDER BY 
	q.mean ASC
