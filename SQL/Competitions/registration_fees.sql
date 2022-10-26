SELECT 
	RANK() OVER (ORDER BY t.fee_€ DESC) as pos,
	t.*
FROM(
	SELECT 
		id, 
		start_date, 
		ROUND(base_entry_fee_lowest_denomination/100,2) as fee_€
	FROM 
		Competitions
	WHERE 
		countryId = 'Italy'
	ORDER 
		BY 3 DESC) t