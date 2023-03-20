SELECT MIN(t.title) AS movie_title
FROM movie_info AS mi,
     title AS t,
     mv443
WHERE mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year > 2005
  AND t.id = mi.movie_id
  AND t.id = mv443.movie_id
  AND mv443.movie_id = mi.movie_id;

