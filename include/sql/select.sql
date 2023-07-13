SELECT sex, population, AVG(weight) AS average_weight
FROM possum
GROUP BY sex, population
ORDER BY average_weight DESC;
