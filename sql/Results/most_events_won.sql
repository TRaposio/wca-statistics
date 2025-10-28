/* Italians ranked by number of wca events won (past and present) */

SELECT 
	t.person_id as wcaid,
	t.person_name as Name,
	COUNT(DISTINCT(t.event_id)) as EventsWon,
	GROUP_CONCAT(DISTINCT(t.event_id) SEPARATOR ', ') as EventList
FROM 
	results t
WHERE 
	t.round_type_id IN ('f','c') AND 
	t.country_id = 'Italy' AND 
	t.pos = 1 AND 
	t.best > 0
GROUP BY 
	t.person_id,
	t.person_name
ORDER BY 
	3 DESC
