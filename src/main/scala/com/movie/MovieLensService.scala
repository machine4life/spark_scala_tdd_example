package com.movie

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object MovieLensService {

  def userMovieRatings(ratingDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val groupedByUser = ratingDF.groupBy(col("USERID")).agg(count("MOVIEID").as("NO_OF_MOVIE"), avg("RATINGS").as("AVG_RATINGS"))
    groupedByUser.withColumn("AVG_RATINGS", bround(col("AVG_RATINGS"), 2))
  }

  def movieCountByGenre(movieDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val explodedGenres = movieDF.withColumn("GENRES", explode(split(col("GENRES"), "[|]")))
    explodedGenres.groupBy(col("GENRES")).agg(countDistinct(col("MOVIEID")).as("TOTAL_MOVIES"))
  }

  def top100Movie(movieDF: DataFrame, ratingDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val groupByRatings = ratingDF.groupBy("MOVIEID").agg(avg("RATINGS").cast(IntegerType).as("AVG_RATINGS")) // => get top 100
    val renameMovieID = groupByRatings.withColumnRenamed("MOVIEID", "GROUPED_MOVIEID")
    val window: WindowSpec = Window.orderBy(col("AVG_RATINGS"), col("MOVIEID"))

    val movieAndRatings = renameMovieID.join(movieDF, col("GROUPED_MOVIEID") === col("MOVIEID"))
      .select(
          col("MOVIEID")
        , col("TITLE")
        , col("AVG_RATINGS")
      ).limit(100)
    movieAndRatings.select(row_number() over (window) as "RANKING", movieAndRatings.col("*"))
  }

}
