SELECT *
FROM (
  SELECT competition_id, 
         round_type_id,
         3 * COUNT(*) AS nsolves,
         IF(3 * COUNT(*) - 
            SUM(IF(value1 < 0, 1, 0)) - 
            SUM(IF(value2 < 0, 1, 0)) - 
            SUM(IF(value3 < 0, 1, 0)) = 1, 1, 0) AS flag, 
         COUNT(IF(regional_single_record IS NOT NULL, 1, NULL)) AS NR_count
  FROM results
  WHERE event_id = ":evento"
  GROUP BY competition_id, round_type_id
  HAVING NR_count > 0
) AS t 
WHERE t.flag = 1
ORDER BY 3 desc;