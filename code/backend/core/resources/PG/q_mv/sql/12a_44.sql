SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS drama_horror_movie
FROM company_name AS cn,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t,
     mv44
WHERE cn.country_code = '[us]'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror')
  AND mi_idx.info > '8.0'
  AND t.production_year BETWEEN 2005 AND 2008
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mv44.movie_id
  AND cn.id = mv44.company_id
  AND mv44.movie_id = mi.movie_id
  AND mv44.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

