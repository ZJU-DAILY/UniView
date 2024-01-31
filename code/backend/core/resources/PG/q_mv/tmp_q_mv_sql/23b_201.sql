SELECT
  MIN(kt.kind) AS movie_kind,
  MIN(t.title) AS complete_nerdy_internet_movie
FROM complete_cast AS cc, comp_cast_type AS cct1, company_name AS cn, company_type AS ct, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_keyword AS mk, title AS t, mv201
WHERE
  'complete+verified' = cct1.kind
  AND '[us]' = cn.country_code
  AND k.keyword IN ('nerd', 'loner', 'alienation', 'dignity')
  AND kt.kind IN ('movie')
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND mv201.movie_id = t.id
  AND mk.movie_id = t.id
  AND mc.movie_id = t.id
  AND cc.movie_id = t.id
  AND mk.movie_id = mv201.movie_id
  AND mc.movie_id = mk.movie_id
  AND cc.movie_id = mk.movie_id
  AND mc.movie_id = mv201.movie_id
  AND cc.movie_id = mv201.movie_id
  AND cc.movie_id = mc.movie_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND cc.status_id = cct1.id