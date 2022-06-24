SELECT 
	personId as WCAID, 
	personName as Name, 
	value1 as solve1,
	value2 as solve2,
	value3 as solve3,
	average, 
	SQRT((POW((value1 - average),2)+POW((value2 - average),2)+POW((value3 - average),2))/3) as std
FROM 
	Results
WHERE 
	eventId = '777' AND 
	average > 0 AND 
	countryId = 'Italy'
ORDER BY 
	std ASC
