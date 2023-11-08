/* fraction of competitors that have a better single world ranking than average world ranking for each event */

SELECT 
	s.eventId as Event,
	SUM(IF(a.worldRank<=s.worldRank, 1, 0))/ COUNT(DISTINCT(s.personId)) as 'single>avg_pct'
FROM 
	RanksSingle s
LEFT JOIN 
	RanksAverage a
ON 
	s.personId = a.personId AND 
	s.eventId = a.eventId
WHERE 
	s.eventId <> '333mbo'AND 
	s.eventId <> 'magic'AND 
	s.eventId <> 'mmagic'AND 
	s.eventId <> '333ft'
GROUP BY 
	s.eventId
ORDER BY 
	2 DESC
