/* Distribution of national records */

SELECT 
	t.person_id, 
	p.name, 
	p.country_id, 
	COUNT(t.event_id) as NRs

FROM
	(SELECT 
		person_id, 
		event_id
	FROM 
		ranks_single 
	WHERE 
		country_rank = 1 AND 
		event_id NOT IN ('magic','mmagic','333ft','333mbo')
	UNION ALL
	SELECT 
		person_id, 
		event_id
	FROM 
		ranks_average
	WHERE 
		country_rank = 1 AND 
		event_id NOT IN ('magic','mmagic','333ft','333mbo')) t,

	persons p

WHERE 
	p.wca_id = t.person_id AND 
	p.sub_id = 1 AND 
	p.country_id = ':country'
GROUP BY 
	t.person_id
ORDER BY 
	NRs DESC


