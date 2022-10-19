# Ranks events of a specific country based on the average world rank of its top N cubers

SELECT 
    q.countryId, 
    q.eventId,
    ROUND(AVG(q.worldRank),2) as avg_wrank_top_N
FROM 
	(SELECT 
		p.countryId, 
    	r.eventId,
    	r.worldRank, 
		RANK() OVER (PARTITION BY r.eventId ORDER BY r.worldRank, r.personId ASC) as truerank
	FROM 
		RanksAverage r, Persons p
	WHERE 
		r.personId = p.id AND 
		p.subid = 1 AND
		p.countryId = ':country'
	) q
WHERE 
	q.truerank <= :N
GROUP BY
	q.eventId
HAVING
    COUNT(DISTINCT q.truerank) = :N
ORDER BY 
	3 ASC