SELECT MIN(mv70.info) AS release_date,
       MIN(mv70.info) AS rating,
       MIN(mv70.title) AS german_movie
FROM mv70
WHERE mv70.country_code ='[de]'
  AND mv70.kind ='movie';

