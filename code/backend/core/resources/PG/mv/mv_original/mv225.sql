select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, keyword.id AS id, keyword.keyword AS keyword, title.episode_nr AS episode_nr, title.title AS title
 from title,keyword,cast_info,movie_keyword
 where (title.id = movie_keyword.movie_id) And (cast_info.movie_id = title.id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'character-name-in-title')