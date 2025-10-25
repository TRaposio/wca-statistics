### Ranks countries by the average world rank of its top N cubers in a specific event

SELECT 
    q.country_id, 
    ROUND(AVG(q.world_rank),2) as avg_wrank_top_N
FROM 
	(SELECT
		p.country_id, 
        r.event_id,
        r.world_rank, 
		RANK() OVER (PARTITION BY p.country_id ORDER BY r.id ASC) as truerank
	FROM 
		ranks_average r, persons p
	WHERE 
		r.person_id = p.wca_id AND 
        r.event_id = ':event' AND
		p.sub_id = 1
	) q
WHERE 
	q.truerank <= :N
GROUP BY
	q.country_id
ORDER BY 
	2 ASC
