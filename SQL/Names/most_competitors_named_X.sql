SELECT 
	competitionId as comp, 
	COUNT(DISTINCT personName) as NameCount
FROM 
	Results
WHERE 
	personName LIKE 'Tommaso%'
GROUP BY 
	competitionId
ORDER BY 2 DESC
