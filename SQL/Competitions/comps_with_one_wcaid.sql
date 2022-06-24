SELECT 
	competitionId as Competition,
	COUNT(DISTINCT(personId)) as Competitors, 
	LEFT(personId,4) as Year
FROM Results
GROUP BY competitionId
HAVING 
	COUNT(DISTINCT(Year)) = 1
