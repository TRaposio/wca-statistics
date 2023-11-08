/* Average of all people that share the same name (at least 5) */

SELECT 
	t.name, 
	ROUND(AVG(t.best)/100,2) as avg, 
	COUNT(DISTINCT t.personId) as persons
FROM (
	SELECT 
		r.best, 
		SUBSTRING_INDEX(p.name,' ', 1) as name, 
		r.personId
	FROM 
		RanksAverage r 
	JOIN 
		Persons p
	ON 
		r.personid = p.wca_id
	WHERE 
		r.eventId = '333' AND 
		p.countryId = 'Italy'
)t
GROUP BY 
	t.name
HAVING 
	persons > 4
ORDER BY 
	avg ASC
