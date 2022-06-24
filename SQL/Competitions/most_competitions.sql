SELECT 
	personId as Id,
	personName as Name, 
	COUNT(DISTINCT competitionId) as Comps
FROM 
	Results
WHERE 
	countryId = 'Italy'
GROUP BY 
	personId
ORDER BY 
	3 DESC
