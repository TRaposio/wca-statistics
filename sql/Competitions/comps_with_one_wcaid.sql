SELECT 
	competition_id as Competition,
	COUNT(DISTINCT(person_id)) as Competitors, 
	LEFT(person_id,4) as Year
FROM results
GROUP BY competition_id
HAVING 
	COUNT(DISTINCT(Year)) = 1
ORDER BY
	Competitors DESC