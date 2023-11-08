/* Competitions in which event A's podium was faster than Event B's ordered by Delta */

WITH podium AS(
	SELECT 
		r.competitionId,
		r.eventId,
		SUM(r.average) as podi
	FROM 
		Results r,
		Competitions c
	WHERE 
		r.competitionId = c.id AND
		r.eventId IN (':eventA',':eventB') AND
		r.pos < 4 AND
		r.roundTypeId IN ('c','f') AND
		r.average > 0 
	GROUP BY 
		r.competitionId,
		r.eventId
	HAVING 
		COUNT(r.pos) = 3)

SELECT 
	eventA.competitionId, 
	ROUND(eventA.podi/100,2) as eventA_podium, 
	ROUND(eventB.podi/100,2) as eventB_podium, 
	ROUND((eventB.podi - eventA.podi)/100,2) as delta
FROM 
	podium as eventA,
	podium as eventB
WHERE 
	eventA.eventId = ':eventA' AND 
	eventB.eventId = ':eventB' AND 
	eventA.competitionId = eventB.competitionId
ORDER BY 
	delta DESC