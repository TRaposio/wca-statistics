/* Competitions in which event A's podium was faster than Event B's ordered by Delta */

WITH podium AS(
	SELECT 
		r.competition_id,
		r.event_id,
		SUM(r.average) as podi
	FROM 
		results r,
		competitions c
	WHERE 
		r.competition_id = c.id AND
		r.event_id IN (':eventA',':eventB') AND
		r.pos < 4 AND
		r.round_type_id IN ('c','f') AND
		r.average > 0 
	GROUP BY 
		r.competition_id,
		r.event_id
	HAVING 
		COUNT(r.pos) = 3)

SELECT 
	eventA.competition_id, 
	ROUND(eventA.podi/100,2) as eventA_podium, 
	ROUND(eventB.podi/100,2) as eventB_podium, 
	ROUND((eventB.podi - eventA.podi)/100,2) as delta
FROM 
	podium as eventA,
	podium as eventB
WHERE 
	eventA.event_id = ':eventA' AND 
	eventB.event_id = ':eventB' AND 
	eventA.competition_id = eventB.competition_id
ORDER BY 
	delta DESC