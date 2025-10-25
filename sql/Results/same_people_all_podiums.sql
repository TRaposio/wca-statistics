/* Competitions in which the same 3 people shared all podiums, ordered by the number of events (at least 2 events) */

SELECT 
	r.competition_id as competition,
    	COUNT(DISTINCT r.event_id) as podiums,
    	GROUP_CONCAT(DISTINCT(r.person_id) SEPARATOR ', ') as IdList,
    	GROUP_CONCAT(DISTINCT(r.person_name) SEPARATOR ', ') as names
FROM 
	results r
WHERE 
	r.round_type_id IN ("f", "c") AND 
	r.pos <= 3 AND 
	r.best >0 
GROUP BY 
	r.competition_id 
HAVING 
	COUNT(DISTINCT r.event_id) >1 AND
	COUNT(DISTINCT r.event_id) = (SELECT COUNT(DISTINCT r2.event_id) from results r2 WHERE r2.competition_id = r.competition_id) AND /* no strange comps */
	COUNT(DISTINCT r.person_id) <=3
ORDER BY
    	podiums DESC
