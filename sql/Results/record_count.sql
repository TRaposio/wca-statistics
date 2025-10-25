/* Italians ranked by the number of records (NR, ER, WR) */

SELECT
	person_id, 
	person_name, 
	SUM(IF(regional_single_record IS NOT NULL,1,0)) + SUM(IF(regional_average_record IS NOT NULL,1,0)) as Records
FROM 
	results
WHERE 
	((regional_single_record IS NOT NULL) OR (regional_average_record IS NOT NULL)) AND
	country_id = 'Italy'
GROUP BY 
	person_id
ORDER BY 
	3 DESC
