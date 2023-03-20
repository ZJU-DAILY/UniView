SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mv423.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(mv423.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn2,
     info_type AS it1,
     kind_type AS kt1,
     kind_type AS kt2,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc2,
     movie_info_idx AS mi_idx1,
     title AS t1,
     mv423
WHERE cn1.country_code = '[us]'
  AND it1.info = 'rating'
  AND mv423.info = 'rating'
  AND kt1.kind IN ('tv series')
  AND kt2.kind IN ('tv series')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mv423.info < '3.0'
  AND mv423.production_year BETWEEN 2005 AND 2008
  AND lt.id = mv423.link_type_id
  AND t1.id = mv423.movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND mv423.movie_id = mi_idx1.movie_id
  AND mv423.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND kt2.id = mv423.kind_id
  AND cn2.id = mc2.company_id
  AND mv423.id = mc2.movie_id
  AND mv423.linked_movie_id = mc2.movie_id
  AND mv423.movie_id = mc2.movie_id;

