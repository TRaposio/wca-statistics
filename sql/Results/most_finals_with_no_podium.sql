/* Italians with no medals ranked by number of finals they qualified for (no combined finals) */

SELECT 
	person_id as wcaid,
	person_name as Name,
	COUNT(DISTINCT(competition_id)) as Competitions
FROM 
	results
WHERE 
	country_id = 'Italy' AND 
	round_type_id in ('c','f') AND 
	best > 0
GROUP BY 
	person_id
HAVING 
	MIN(pos) > 3
ORDER BY 
	Competitions DESC
