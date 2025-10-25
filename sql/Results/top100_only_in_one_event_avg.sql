/* Events ranked by percentage of the top 100 that are top 100 only in that event */

SELECT 
	event_id, 
	ROUND(COUNT(event_id)/100,2) as onlytop100_pct
FROM (
	SELECT 
		person_id as id, 
		event_id
	FROM 
		ranks_average
	WHERE 
		world_rank <= 100 AND 
		event_id NOT IN ('magic','mmagic','333ft','333mbo')
	GROUP BY person_id
	HAVING COUNT(event_id) = 1) p
GROUP BY 
	event_id
ORDER BY 
	2 DESC
