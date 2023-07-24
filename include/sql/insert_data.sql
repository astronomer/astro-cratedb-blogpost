INSERT INTO doc.possum (case_num, site, pop, sex, age, hdlngth, skullw, totlngth, taill, footlgth, earconch, eye, chest, belly)
SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n
FROM UNNEST(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS t(a, b, c, d, e, f, g, h, i, j, k, l, m, n);
