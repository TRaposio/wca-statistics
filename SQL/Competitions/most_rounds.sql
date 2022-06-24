SELECT 
	p.personId as Id, 
	personName as Name, 
	SUM(p.TimesHeld) as Rounds 
FROM 
	(SELECT 
		personId, 
		personName, 
		COUNT(DISTINCT roundTypeId) as TimesHeld
	FROM 
		Results r
	JOIN 
		Competitions c
	ON 
		r.competitionId = c.id
	WHERE 
		c.countryId = 'Italy'
	GROUP BY 
		personId, 
		competitionId)p
GROUP BY 
	p.personId
ORDER BY 
	Rounds DESC
