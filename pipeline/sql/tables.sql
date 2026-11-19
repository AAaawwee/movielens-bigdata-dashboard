CREATE DATABASE IF NOT EXISTS movielens;
USE movielens;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_ratings (
    user_id INT, movie_id INT, rating DOUBLE, `timestamp` BIGINT
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/clean/ratings';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_movies (
    movie_id INT, title STRING, genres STRING
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/clean/movies';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_users (
    user_id INT, gender STRING, age_group INT, occupation INT, zip_code STRING
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/clean/users';

CREATE EXTERNAL TABLE IF NOT EXISTS dws_movie_stats (
    movie_id INT, title STRING, genres STRING, rating_count BIGINT, rater_count BIGINT, avg_rating DOUBLE
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/result/offline/movie_stats';

CREATE EXTERNAL TABLE IF NOT EXISTS dws_genre_stats (
    genre STRING, movie_count BIGINT, rating_count BIGINT, avg_rating DOUBLE
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/result/offline/genre_stats';

CREATE EXTERNAL TABLE IF NOT EXISTS ads_recommendations (
    user_id INT, movie_id INT, predicted_rating FLOAT, recommendation_rank INT
) STORED AS PARQUET LOCATION '${hivevar:movie_root}/result/recommendations';
