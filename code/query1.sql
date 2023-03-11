SELECT quotes.symbol, companies.name
FROM quotes
INNER JOIN companies ON quotes.symbol = companies.symbol
WHERE price/avg_price = (
   SELECT MAX(price/avg_price)
   FROM quotes
   )
LIMIT 1