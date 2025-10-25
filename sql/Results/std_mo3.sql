/* Italian 7x7 means ranked by ascending standard deviation */

SELECT 
	person_id as WCAID, 
	person_name as Name,
	competition_id as Comp,
	value1 as solve1,
	value2 as solve2,
	value3 as solve3,
	average, 
	SQRT((POW((value1 - average),2)+POW((value2 - average),2)+POW((value3 - average),2))/3) as std
FROM 
	results
WHERE 
	event_id = '777' AND 
	average > 0 AND 
	country_id = 'Italy'
ORDER BY 
	std ASC
