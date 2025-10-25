/* Most championships attended */

SELECT 
	r.person_id, 
	r.person_name as name, 
	COUNT(DISTINCT r.competition_id) as championships
FROM 
	results r,
    championships s
WHERE 
	r.competition_id = s.competition_id
GROUP BY 
	r.person_id
ORDER BY 
	3 DESC
