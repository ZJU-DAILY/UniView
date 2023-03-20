SELECT MIN(mv482.name) AS of_person,
       MIN(mv482.title) AS biography_movie
FROM mv482
WHERE mv482.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (mv482.gender='m'
       OR (mv482.gender = 'f'
           AND mv482.name LIKE 'B%'))
  AND mv482.production_year BETWEEN 1980 AND 1995

