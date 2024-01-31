create materialized view if not exists mv264 as select keyword.id AS keyword_id_0, keyword.keyword AS keyword, link_type.id AS link_type_id_2, link_type.link AS link, movie_info.info AS info, movie_info.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from link_type,movie_info,keyword,title,movie_link,movie_keyword
 where (movie_link.link_type_id = link_type.id) And (link_type.link like '%follow%') And (movie_info.movie_id = movie_link.movie_id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'English')) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'sequel') And (title.id = movie_keyword.movie_id) And (movie_link.movie_id = title.id) And (movie_keyword.movie_id = movie_link.movie_id)