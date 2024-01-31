select link_type.id AS id, link_type.link AS link, movie_keyword.keyword_id AS keyword_id, movie_keyword.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from movie_link,title,link_type,movie_keyword
 where (title.id = movie_link.movie_id) And (movie_link.link_type_id = link_type.id) And (movie_keyword.movie_id = title.id) And (title.title like '%Money%')  And  (title.production_year = 1998) And (link_type.link like '%follows%')