SELECT MIN(mv70.name) AS producing_company,
       MIN(mv70.info) AS rating,
       MIN(mv70.title) AS movie_about_winning
FROM mv70
WHERE mv70.country_code ='[us]'
  AND mv70.kind ='movie'
  AND mv70.title != ''
  AND (mv70.title LIKE 'Champion%'
       OR mv70.title LIKE 'Loser%');


