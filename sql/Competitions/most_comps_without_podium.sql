SELECT
	p.id,
	p.name,
	p.comps
FROM (
	SELECT 
		person_name name,
		person_id id,
		COUNT(DISTINCT competition_id) comps 
	FROM 
		results 
	WHERE 
		country_id =":country" 
	GROUP by 
		person_id, 
		person_name
	) p 
WHERE NOT EXISTS (
		SELECT 
			r.person_id
		FROM 
			results r
		WHERE 
			p.id=r.person_id AND 
			pos < 4 AND 
			r.round_type_id IN ('c','f') AND best >0)
ORDER BY 
	3
DESC LIMIT 
	20
