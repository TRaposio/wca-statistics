SELECT 
	personId as person, 
	best as Avg
FROM RanksAverage
WHERE 
	eventId = '333oh' AND 
	personId IN (
			SELECT personId
			FROM RanksAverage
			WHERE eventId = '333' AND best >= 1000)
