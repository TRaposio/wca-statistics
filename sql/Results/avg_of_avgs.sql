SELECT 
	person_id, 
	person_name, 
	competition_id, 
	ROUND(AVG(average)/100, 2) as Media_medie
FROM 
	results
WHERE 
	country_id = "Italy" AND 
	event_id = "333"
GROUP BY 
	competition_id, 
	person_id
HAVING 
	MIN(average) > 0
ORDER BY 
	4 ASC