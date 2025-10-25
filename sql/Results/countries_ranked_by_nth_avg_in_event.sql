SELECT 
        q.country_id, 
        wca_statistics_time_format(q.mean, q.event_id, 'average') as Nth_average
FROM 
	(SELECT 
		p.country_id, 
		r.best as mean,
        r.event_id, 
		RANK() OVER (PARTITION BY p.country_id ORDER BY r.world_rank, r.person_id ASC) as truerank
	FROM 
		ranks_average r, persons p
	WHERE 
		r.person_id = p.wca_id AND 
        r.event_id = ':event' AND
		p.sub_id = 1
	) q
WHERE 
	q.truerank = :N
ORDER BY 
	q.mean ASC
