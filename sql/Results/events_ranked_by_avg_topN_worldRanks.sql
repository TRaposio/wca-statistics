# Ranks events of a specific country based on the average world rank of its top N cubers

SELECT 
    q.country_id, 
    q.event_id,
    ROUND(AVG(q.world_rank),2) as avg_wrank_top_N
FROM 
	(SELECT 
		p.country_id, 
    	r.event_id,
    	r.world_rank, 
		RANK() OVER (PARTITION BY r.event_id ORDER BY r.world_rank, r.person_id ASC) as truerank
	FROM 
		ranks_average r, persons p
	WHERE 
		r.person_id = p.wca_id AND 
		p.sub_id = 1 AND
		p.country_id = ':country' AND
		r.event_id NOT IN ('magic','mmagic','333ft','333mbo')
	) q
WHERE 
	q.truerank <= :N
GROUP BY
	q.event_id
HAVING
    COUNT(DISTINCT q.truerank) = :N
ORDER BY 
	3 ASC
