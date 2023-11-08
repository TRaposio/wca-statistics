### Ranks countries by the average world rank of its top N cubers in a specific event

SELECT 
    q.countryId, 
    ROUND(AVG(q.worldRank),2) as avg_wrank_top_N
FROM 
	(SELECT 
		p.countryId, 
        r.eventId,
        r.worldRank, 
		RANK() OVER (PARTITION BY p.countryId ORDER BY r.id ASC) as truerank
	FROM 
		RanksAverage r, Persons p
	WHERE 
		r.personId = p.wca_id AND 
        	r.eventId = ':event' AND
		p.subid = 1
	) q
WHERE 
	q.truerank <= :N
GROUP BY
	q.countryId
ORDER BY 
	2 ASC
