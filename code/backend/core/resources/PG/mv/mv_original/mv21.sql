select keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_info.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from title,movie_info,keyword,movie_keyword
 where (title.id = movie_keyword.movie_id) And (movie_info.movie_id = title.id) And (movie_info.info = 'Bulgaria') And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword like '%sequel%')