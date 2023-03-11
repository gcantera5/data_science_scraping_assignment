SELECT companies.name
FROM quotes
INNER JOIN companies ON quotes.symbol = companies.symbol
WHERE prev_close > avg_price
ORDER BY price DESC
LIMIT 1