SELECT 
	c.id as competition, 
	IF(r.eventId = '333fm', ROUND(SUM(r.average)/100,2), wca_statistics_time_format(SUM(r.average), 'r.eventId', 'average')) as podium,
	GROUP_CONCAT(IF(r.pos=1,r.personName, NULL)) as first,
        GROUP_CONCAT(IF(r.pos=1,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result1,
	GROUP_CONCAT(IF(r.pos=2,r.personName, NULL)) as second,
        GROUP_CONCAT(IF(r.pos=2,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result2,
	GROUP_CONCAT(IF(r.pos=3,r.personName, NULL)) as third,
        GROUP_CONCAT(IF(r.pos=3,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result3
FROM 
	Competitions c, 
	Results r
WHERE 
	c.id = r.competitionId AND 
	r.eventId = ':event' AND 
	r.pos < 4 AND 
	r.average > 0 AND 
	r.roundTypeId IN ('c','f') AND 
	c.countryId = ':country'
GROUP BY 
	c.id
HAVING
        COUNT(DISTINCT personId) = 3
ORDER BY 
	SUM(r.average) ASC
