SELECT
  MIN(mc.note) AS production_note,
  MIN(t.title) AS movie_title,
  MIN(t.production_year) AS movie_year
FROM company_type AS ct, movie_companies AS mc, title AS t, mv6
WHERE
  'production companies' = ct.kind
  AND NOT mc.note LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year > 2000
  AND ct.id = mc.company_type_id
  AND mc.movie_id = t.id
  AND mv6.movie_id = t.id
  AND mc.movie_id = mv6.movie_id