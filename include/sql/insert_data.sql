INSERT INTO doc.possum (case_num, site, pop, sex, age, hdlngth, skullw, totlngth, taill, footlgth, earconch, eye, chest, belly)
SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n
FROM UNNEST(%s) a, UNNEST(%s) b, UNNEST(%s) c, UNNEST(%s) d, UNNEST(%s) e, 
     UNNEST(%s) f, UNNEST(%s) g, UNNEST(%s) h, UNNEST(%s) i, UNNEST(%s) j, 
     UNNEST(%s) k, UNNEST(%s) l, UNNEST(%s) m, UNNEST(%s) n;
