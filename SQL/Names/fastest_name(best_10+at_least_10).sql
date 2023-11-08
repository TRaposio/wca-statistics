/* average of top 10 Italians for each name with at least 10 competitors */

SELECT 
	t.name, 
	ROUND(AVG(t.best)/100,2) as avg
FROM (
	SELECT 
		*, 
		RANK() OVER (PARTITION BY q.name ORDER BY q.best ASC) as name_rank
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
		)q
	)t
WHERE 
	t.name_rank <= 10
GROUP BY 
	t.name
HAVING 
	COUNT(DISTINCT t.personId) = 10
ORDER BY 
	avg ASC
