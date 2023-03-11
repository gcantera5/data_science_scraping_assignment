SELECT location, COUNT(*)
FROM companies
GROUP BY location
ORDER BY location ASC