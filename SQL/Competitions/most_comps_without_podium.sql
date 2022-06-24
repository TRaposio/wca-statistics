SELECT
	p.id,
	p.name,
	p.comps
FROM (
	SELECT 
		personName name,
		personId id,
		COUNT(DISTINCT competitionId) comps 
	FROM 
		Results 
	WHERE 
		CountryId ="Italy" 
	GROUP by 
		personId, 
		personName
	) p 
WHERE NOT EXISTS (
		SELECT 
			r.personId
		FROM 
			Results r
		WHERE 
			p.id=r.personId AND 
			pos < 4 AND 
			r.roundTypeId IN ('c','f') AND best >0)
ORDER BY 
	3
DESC LIMIT 
	20
