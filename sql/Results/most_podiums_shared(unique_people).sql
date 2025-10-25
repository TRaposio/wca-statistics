/* Italians ranked by the number of unique people they shared a podium with */

SELECT 
	a.person_id as aID, 
	a.person_name as Name, 
	COUNT(DISTINCT b.person_id) as On_podium_with
FROM (
	SELECT * 
	FROM 
		results r 
	WHERE 
		r.round_type_id IN ('f','c') AND 
		r.best > 0 AND 
		r.pos < 4 AND
		r.country_id = 'Italy'
	)a
JOIN (
	SELECT * 
	FROM 
		results q 
	WHERE 
		q.round_type_id IN ('f','c') AND 
		q.best > 0 AND 
		q.pos < 4
	)b
ON 
	a.competition_id = b.competition_id AND 
	a.event_id = b.event_id AND 
	a.person_id <> b.person_id
GROUP BY 
	a.person_id
ORDER BY 
	3 DESC
LIMIT 20
