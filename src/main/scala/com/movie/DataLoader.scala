package com.movie

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType

object DataLoader {
  val userSchema = StructType(
    Seq(
      StructField(name = "USERID", dataType = IntegerType, nullable = true),
      StructField(name = "GENDER", dataType = StringType, nullable = true),
      StructField(name = "AGE", dataType = IntegerType, nullable = true),
      StructField(name = "OCCUPATION", dataType = IntegerType, nullable = true),
      StructField(name = "ZIPCODE", dataType = StringType, nullable = true)
    )
  )

  val movieSchema = StructType(
    Seq(
      StructField(name = "MOVIEID", dataType = IntegerType, nullable = true),
      StructField(name = "TITLE", dataType = StringType, nullable = true),
      StructField(name = "GENRES", dataType = StringType, nullable = true)
    )
  )

  val ratingSchema = StructType(
    Seq(
      StructField(name = "USERID", dataType = IntegerType, nullable = true),
      StructField(name = "MOVIEID", dataType = IntegerType, nullable = true),
      StructField(name = "RATINGS", dataType = IntegerType, nullable = true),
      StructField(name = "TS", dataType = LongType, nullable = true)
    )
  )

  def loadUsers(file: String)(implicit spark: SparkSession): DataFrame = {
    def row(line: Array[String]): Row = Row(line(0).toInt, line(1), line(2).toInt, line(3).toInt, line(4))
    getDataFrame(file, row, userSchema)
  }

  def loadMovies(file: String)(implicit spark: SparkSession): DataFrame = {
    def row(line: Array[String]): Row = Row(line(0).toInt, line(1), line(2))
    getDataFrame(file, row, movieSchema)
  }

  def loadRatings(file: String)(implicit spark: SparkSession): DataFrame = {
    def row(line: Array[String]): Row = Row(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toLong)
    getDataFrame(file, row, ratingSchema)
  }


  def getDataFrame(file: String, row: (Array[String]) => Row, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val rawData = spark.sparkContext.textFile(file)
    val rowRDD = rawData.map(line => line.split("::")).map(line => row(line))
    spark.createDataFrame(rowRDD, schema)
  }

}
