# Per AO5 restituisce la BPA, per mo3 restituisce la somma dei primi due tentativi

SELECT 
   eventId,
   competitionId, 
   roundTypeId, 
   value1,
   value2,
   value3, 
   IF(
      eventId IN ('222','333','444','555','pyram','skewb','minx','333oh','clock', 'sq1'), 
      value4, 
      '-') as value4,
   IF(
      eventId IN ('222','333','444','555','pyram','skewb','minx','333oh','clock', 'sq1'), 
      CASE
         WHEN value1<0 THEN (value2+value3+value4)/300
         WHEN value2<0 THEN (value1+value3+value4)/300 
         WHEN value3<0 THEN (value2+value1+value4)/300 
         WHEN value4<0 THEN (value2+value3+value1)/300 
         ELSE (value1+value2+value3+value4-GREATEST(value1,value2,value3,value4))/300
      END, 
      (value1+value2) / 100
      ) as BPA,
   average/100 as average
FROM 
   Results
WHERE 
   personId = ':WCAID' AND 
   eventId = ':evento' AND 
   average > 0 
ORDER BY 
   BPA ASC