/* Competition Sweeps: people that won gold in all events of the competition */
/* The second check in the HAVING ensures that an all DNF podium + a sweep in all other events is counted */

SELECT 
	r.competitionId as competition,
	r.personId,
	r.personName,
	COUNT(DISTINCT r.eventId) as events_swept
FROM 
	Results r
WHERE 
	r.roundTypeId IN ("f", "c") AND 
	r.pos = 1 AND 
	r.best > 0 
GROUP BY 
	r.competitionId 
HAVING 
	COUNT(DISTINCT r.eventId) > 1 AND
	COUNT(DISTINCT r.eventId) = (SELECT COUNT(DISTINCT eventId) from Results r2 WHERE r2.competitionId = r.competitionId) AND
	COUNT(DISTINCT r.personId) = 1
ORDER BY
 	events_swept DESC