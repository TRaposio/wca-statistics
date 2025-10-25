/* fraction of competitors that have a better average world ranking than single world ranking for each event */

SELECT 
	s.event_id as Event,
	SUM(IF(a.world_rank<=s.world_rank, 1, 0))/ COUNT(DISTINCT(s.person_id)) as 'single>avg_pct'
FROM 
	ranks_single s
LEFT JOIN 
	ranks_average a
ON 
	s.person_id = a.person_id AND 
	s.event_id = a.event_id
WHERE 
	s.event_id <> '333mbo'AND 
	s.event_id <> 'magic'AND 
	s.event_id <> 'mmagic'AND 
	s.event_id <> '333ft'
GROUP BY 
	s.event_id
ORDER BY 
	2 DESC
