/* Italian 3x3 average ranked by ascending standard deviation */

SELECT
   competition_id as Competition,
   person_id as WCAID, 
   person_name as Name, 
   value1 as solve1,
   value2 as solve2,
   value3 as solve3,
   value4 as solve4,
   value5 as solve5,
   average,
   CASE 
      WHEN (value1 >= value2 AND value1 >= value3 AND value1 >= value4 AND value1 >= value5) OR value1<0
      THEN SQRT((POW(value2 - average,2)+POW(value3 - average,2)+POW(value4 - average,2)+POW(value5 - 
      average,2)-POW(best-average,2))/3) 
      WHEN (value2 >= value1 AND value2 >= value3 AND value2 >= value4 AND value2 >= value5) OR value2<0
      THEN SQRT((POW(value1 - average,2)+POW(value3 - average,2)+POW(value4 - average,2)+POW(value5 - 
      average,2)-POW(best-average,2))/3) 
      WHEN (value3 >= value1 AND value3 >= value2 AND value3 >= value4 AND value3 >= value5) OR value3<0
      THEN SQRT((POW(value1 - average,2)+POW(value2 - average,2)+POW(value4 - average,2)+POW(value5 - 
      average,2)-POW(best-average,2))/3) 
      WHEN (value4 >= value1 AND value4 >= value2 AND value4 >= value3 AND value4 >= value5) OR value4<0
      THEN SQRT((POW(value1 - average,2)+POW(value2 - average,2)+POW(value3 - average,2)+POW(value5 - 
      average,2)-POW(best-average,2))/3) 
      WHEN (value5 >= value1 AND value5 >= value2 AND value5 >= value3 AND value5 >= value4) OR value5<0
      THEN SQRT((POW(value1 - average,2)+POW(value2 - average,2)+POW(value3 - average,2)+POW(value4 - 
      average,2)-POW(best-average,2))/3) 
      ELSE NULL
END as std
FROM 
   results
WHERE 
   event_id = '333' AND 
   average > 0 AND 
   country_id = 'Italy'
ORDER BY 
   10 ASC
