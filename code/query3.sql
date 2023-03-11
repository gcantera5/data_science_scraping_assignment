SELECT quotes.symbol, companies.name
FROM quotes
INNER JOIN companies ON quotes.symbol = companies.symbol
WHERE price > 35 AND ABS(price - avg_price) < 5
ORDER BY ABS(price - avg_price) ASC