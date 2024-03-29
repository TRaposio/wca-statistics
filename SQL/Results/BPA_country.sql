# Per AO5 restituisce la BPA, per mo3 restituisce la somma dei primi due tentativi

SELECT 
   eventId,
   personId,
   personName,
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
         WHEN value1<0 THEN ROUND((value2+value3+value4)/300,2)
         WHEN value2<0 THEN ROUND((value1+value3+value4)/300,2)
         WHEN value3<0 THEN ROUND((value2+value1+value4)/300,2)
         WHEN value4<0 THEN ROUND((value2+value3+value1)/300,2)
         ELSE ROUND((value1+value2+value3+value4-GREATEST(value1,value2,value3,value4))/300,2)
      END, 
      (value1+value2) / 100
      ) as BPA,
   ROUND(average/100,2) as average
FROM 
   Results
WHERE 
   countryId = 'Italy' AND 
   eventId = ':evento' AND 
   average > 0 
ORDER BY 
   BPA ASC