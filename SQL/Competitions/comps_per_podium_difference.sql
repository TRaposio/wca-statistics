/*Competitions in which the pyraminx podium was faster than 2x2 podium ordered by delta*/

WITH podium AS(
SELECT r.competitionId, r.eventId, SUM(r.average) as podi
FROM Results r, Competitions c
WHERE r.competitionId = c.id
AND r.eventId IN ('pyram','222')
AND r.pos < 4
AND r.roundTypeId IN ('c','f')
AND r.average > 0
GROUP BY r.competitionId, r.eventId
HAVING COUNT(r.pos) = 3)

SELECT pyra.competitionId, ROUND(pyra.podi/100,2) as pyrapodium, ROUND(due.podi/100,2) as duepodium, ROUND((due.podi - pyra.podi)/100,2) as delta
FROM podium as pyra, podium as due
WHERE pyra.eventId = 'pyram' AND due.eventId = '222' AND pyra.competitionId = due.competitionId
ORDER BY delta DESC
