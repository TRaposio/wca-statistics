SELECT 
	id, 
	DATEDIFF(CAST(results_submitted_at AS DATE), end_date) as Result_submission
FROM 
	Competitions
WHERE 
    countryId = 'Italy' AND 
    year > 2020 AND 
    results_submitted_at IS NOT NULL
ORDER BY 
	2 ASC