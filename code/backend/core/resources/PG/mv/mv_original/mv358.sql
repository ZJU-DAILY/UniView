select complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, complete_cast.subject_id AS subject_id, keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_link.link_type_id AS link_type_id, title.production_year AS production_year, title.title AS title
 from movie_keyword,movie_info,title,complete_cast,movie_link,keyword
 where (title.id = movie_keyword.movie_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.movie_id = movie_link.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_info.info in ('Sweden', 'Germany', 'Swedish', 'German')) And (movie_link.movie_id = title.id) And (keyword.keyword = 'sequel')