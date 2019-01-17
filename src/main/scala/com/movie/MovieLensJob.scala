package com.movie

import org.apache.spark.sql.{SparkSession}

object MovieLensJob {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = getSparkSession("MovieJob")

    val users = DataLoader.loadUsers("/tmp/users.dat")
    val movies = DataLoader.loadMovies("/tmp/movies.dat")
    val ratings = DataLoader.loadRatings("/tmp/ratings.dat")

    val userMovieDF = MovieLensService.userMovieRatings(ratings)
    userMovieDF.show(5)
    userMovieDF.coalesce(1).write.option("header", "true").csv("/tmp/user_output")

    val movieGenres = MovieLensService.movieCountByGenre(movies)
    movieGenres.show(5)
    movieGenres.coalesce(1).write.option("header", "true").csv("/tmp/movie_genres")

    val top100 = MovieLensService.top100Movie(movies, ratings)
    top100.show()
    top100.coalesce(1).write.parquet("/tmp/top100")

  }

  def getSparkSession(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Movie Ratings")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
