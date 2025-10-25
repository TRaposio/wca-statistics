/* Competition Sweeps: people that won gold in all events of the competition */
/* The second check in the HAVING ensures that an all DNF podium + a sweep in all other events is counted */

SELECT 
	r.competition_id as competition,
	r.person_id,
	r.person_name,
	COUNT(DISTINCT r.event_id) as events_swept
FROM 
	results r
WHERE 
	r.round_type_id IN ("f", "c") AND 
	r.pos = 1 AND 
	r.best > 0 
GROUP BY 
	r.competition_id 
HAVING 
	COUNT(DISTINCT r.event_id) > 1 AND
	COUNT(DISTINCT r.event_id) = (SELECT COUNT(DISTINCT event_id) from results r2 WHERE r2.competition_id = r.competition_id) AND
	COUNT(DISTINCT r.person_id) = 1
ORDER BY
 	events_swept DESC