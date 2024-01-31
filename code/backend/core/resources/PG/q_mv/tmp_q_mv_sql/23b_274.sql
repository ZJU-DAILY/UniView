SELECT
  MIN(kt.kind) AS movie_kind,
  MIN(t.title) AS complete_nerdy_internet_movie
FROM company_name AS cn, company_type AS ct, info_type AS it1, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, title AS t, mv274
WHERE
  '[us]' = cn.country_code
  AND 'release dates' = it1.info
  AND k.keyword IN ('nerd', 'loner', 'alienation', 'dignity')
  AND kt.kind IN ('movie')
  AND mi.note LIKE '%internet%'
  AND mi.info LIKE 'USA:% 200%'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND mi.movie_id = t.id
  AND mk.movie_id = t.id
  AND mc.movie_id = t.id
  AND mv274.movie_id = t.id
  AND mi.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id
  AND mk.movie_id = mv274.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = mv274.movie_id
  AND mc.movie_id = mv274.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id