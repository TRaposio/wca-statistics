SELECT 
	id, 
	DATEDIFF(CAST(results_submitted_at AS DATE), end_date) as Result_submission
FROM 
	competitions
WHERE 
    country_id = ':country' AND 
    RIGHT(id,4) > 2020 AND 
    results_submitted_at IS NOT NULL
ORDER BY 
	2 ASC
