SELECT sex, pop, AVG(taill) AS average_tail_length
FROM doc.possum
GROUP BY sex, pop
ORDER BY average_tail_length DESC;
