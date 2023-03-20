SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     movie_companies AS mc,
     title AS t,
     mv52
WHERE ct.kind = 'production companies'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mv52.movie_id
  AND mc.movie_id = mv52.movie_id

