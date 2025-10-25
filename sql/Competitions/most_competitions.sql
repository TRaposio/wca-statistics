SELECT 
	person_id as Id,
	person_name as Name, 
	COUNT(DISTINCT competition_id) as Comps
FROM 
	results
WHERE 
	country_id = ':country'
GROUP BY 
	person_id
ORDER BY 
	3 DESC
