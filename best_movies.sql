select 
    title, movieId
from
    movies
where 
    movieId in (
        SELECT
            movieId, count(*)
        FROM
            ratings
        WHERE
            rating > 3
        GROUP BY
            movieId
        ORDER BY
            2 desc
        LIMIT 10
    ) 
ORDER BY
    title
