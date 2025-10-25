SELECT 
	r.person_id,
	r.person_name as name,
	RIGHT(r.competition_id,4) as year,
	COUNT(DISTINCT r.competition_id) as championships
FROM 
	results r, 
    championships s
WHERE 
	r.competition_id = s.competition_id
GROUP BY 
	r.person_id, 
	RIGHT(r.competition_id,4)
ORDER BY 
	4 DESC

