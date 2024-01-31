select keyword.id AS id, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id, title.title AS title
 from movie_companies,title,keyword,movie_keyword
 where (movie_companies.movie_id = title.id) And (title.id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'character-name-in-title')