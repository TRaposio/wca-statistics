##### Grouped view (list of names, list of times)

SELECT 
	c.id, 
	ROUND(sum(r.average)/100,2) as podio,
	GROUP_CONCAT(r.personName ORDER BY average ASC SEPARATOR ', ') as persone,
	GROUP_CONCAT(ROUND(r.average/100,2) ORDER BY average ASC SEPARATOR ', ') as medie
FROM 
	Competitions c, 
	Results r
WHERE 
	c.id = r.competitionId AND 
	r.eventId = 'pyram' AND 
	r.pos < 4 AND 
	r.average > 0 AND 
	r.roundTypeId IN ('c','f') AND 
	c.countryId = 'Italy'
GROUP BY 
	c.id
ORDER BY 
	2 ASC


###############################################
##### Single view (a column for each name and each time)

SELECT 
	c.id as competition, 
	ROUND(sum(r.average)/100,2) as podium,
	GROUP_CONCAT(IF(r.pos=1,r.personName, NULL)) as first,
    GROUP_CONCAT(IF(r.pos=1,ROUND(r.average/100,2), NULL)) as result1,
	GROUP_CONCAT(IF(r.pos=2,r.personName, NULL)) as second,
    GROUP_CONCAT(IF(r.pos=2,ROUND(r.average/100,2), NULL)) as result2,
	GROUP_CONCAT(IF(r.pos=3,r.personName, NULL)) as third,
    GROUP_CONCAT(IF(r.pos=3,ROUND(r.average/100,2), NULL)) as result3
FROM 
	Competitions c, 
	Results r
WHERE 
	c.id = r.competitionId AND 
	r.eventId = 'pyram' AND 
	r.pos < 4 AND 
	r.average > 0 AND 
	r.roundTypeId IN ('c','f') AND 
	c.countryId = 'Italy'
GROUP BY 
	c.id
ORDER BY 
	2 ASC