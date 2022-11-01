SELECT 
	personId, 
	personName, 
	competitionId, 
	ROUND(AVG(average)/100, 2) as Media_medie
FROM 
	Results
WHERE 
	countryId = "Italy" AND 
	eventId = "333"
GROUP BY 
	competitionId, 
	personId
HAVING 
	MIN(average) > 0
ORDER BY 
	4 ASC