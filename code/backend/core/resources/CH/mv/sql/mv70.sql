select keyword.id AS id, movie_keyword.movie_id AS movie_id, keyword.keyword AS keyword
 from movie_keyword,keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = '10,000-mile-club')