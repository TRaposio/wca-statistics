/* Competitions in which the same 3 people shared all podiums, ordered by the number of events (at least 2 events) */

SELECT 
	r.competitionId as competition,
    	COUNT(DISTINCT r.eventId) as podiums,
    	GROUP_CONCAT(DISTINCT(r.personId) SEPARATOR ', ') as IdList,
    	GROUP_CONCAT(DISTINCT(r.personName) SEPARATOR ', ') as names
FROM 
	Results r
WHERE 
	r.roundTypeId IN ("f", "c") AND 
	r.pos <= 3 AND 
	r.best >0 
GROUP BY 
	r.competitionId 
HAVING 
	COUNT(DISTINCT r.eventId) >1 AND
	COUNT(DISTINCT r.eventId) = (SELECT COUNT(DISTINCT r2.eventId) from Results r2 WHERE r2.competitionId = r.competitionId) AND /* no strange comps */
	COUNT(DISTINCT r.personId) <=3
ORDER BY
    	podiums DESC
