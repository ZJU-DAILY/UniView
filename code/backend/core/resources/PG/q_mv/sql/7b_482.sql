SELECT MIN(mv482.name) AS of_person,
       MIN(mv482.title) AS biography_movie
FROM mv482
WHERE mv482.name_pcode_cf LIKE 'D%'
  AND mv482.gender='m'
  AND mv482.production_year BETWEEN 1980 AND 1984;

