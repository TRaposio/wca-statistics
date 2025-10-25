SELECT 
	c.id as competition, 
	IF(r.event_id = '333fm', ROUND(SUM(r.average)/100,2), wca_statistics_time_format(SUM(r.average), 'r.eventId', 'average')) as podium,
	GROUP_CONCAT(IF(r.pos=1,r.person_name, NULL)) as first,
        GROUP_CONCAT(IF(r.pos=1,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result1,
	GROUP_CONCAT(IF(r.pos=2,r.person_name, NULL)) as second,
        GROUP_CONCAT(IF(r.pos=2,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result2,
	GROUP_CONCAT(IF(r.pos=3,r.person_name, NULL)) as third,
        GROUP_CONCAT(IF(r.pos=3,wca_statistics_time_format(r.average, 'r.eventId', 'average'), NULL)) as result3
FROM 
	competitions c, 
	results r
WHERE 
	c.id = r.competition_id AND 
	r.event_id = ':event' AND 
	r.pos < 4 AND 
	r.average > 0 AND 
	r.round_type_id IN ('c','f') AND 
	c.country_id = ':country'
GROUP BY 
	c.id
HAVING
        COUNT(DISTINCT person_id) = 3
ORDER BY 
	SUM(r.average) ASC
