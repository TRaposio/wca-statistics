#Super easy code

SELECT 
	t.personId, 
	p.name, 
	p.countryId, 
	COUNT(t.eventId) as NRs

FROM
	(SELECT 
		personId, 
		eventId
	FROM 
		RanksSingle 
	WHERE 
		countryRank = 1 AND 
		eventId NOT IN ('magic','mmagic','333ft','333mbo')
	UNION ALL
	SELECT 
		personId, 
		eventId
	FROM 
		RanksAverage
	WHERE 
		countryRank = 1 AND 
		eventId NOT IN ('magic','mmagic','333ft','333mbo')) t,

	Persons p

WHERE 
	p.id = t.personId AND 
	p.subId = 1 AND 
	p.countryId = ':country'
GROUP BY 
	t.personId
ORDER BY 
	t.NRs DESC


# Super long and dumb code, outer joins were necessary to simulate a full
# outer join since people with either only single records or average
# records get lost in a simple join


WITH
avg AS(
	SELECT
		personId, 
		COUNT(DISTINCT eventId) as na
	FROM 
		RanksAverage
	WHERE 
		countryRank = 1 AND
		eventId NOT IN ('magic','mmagic','333ft','333mbo')
	GROUP BY 
		personId
),
single AS(
	SELECT
		personId, 
		COUNT(DISTINCT eventId) as ns
	FROM 
		RanksSingle
	WHERE 
		countryRank = 1 AND
		eventId NOT IN ('magic','mmagic','333ft','333mbo')
	GROUP BY 
		personId
)
SELECT 
	p.id, 
	p.name, 
	p.countryId, 
	t.ns + t.na as total_NR
FROM
	(SELECT 
		s.personId, 
		IF(s.ns IS NULL, 0, s.ns) as ns, 
		IF(a.na IS NULL, 0, a.na) as na
	FROM 
		single s
	LEFT OUTER JOIN 
		avg a 
	ON 
		s.personId = a.personId

	UNION

	SELECT 
		a.personId, 
		IF(s.ns IS NULL, 0, s.ns) as ns, 
		IF(a.na IS NULL, 0, a.na) as na
	FROM 
		avg a
	LEFT OUTER JOIN 
		single s 
	ON 
		s.personId = a.personId) t, 
	Persons p

WHERE 
	p.subId = 1 AND 
	p.countryId = 'Italy' AND 
	p.id = t.personId
ORDER BY 
	total_NR DESC

