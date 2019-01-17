package com.movie

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.{FunSuite}

class MovieLensServiceTest extends FunSuite with DatasetSuiteBase {

  lazy implicit val mySpark = spark
  import spark.implicits._

  test("No of Movie and Avg Rating By Users") {
    spark.sparkContext.setLogLevel("ERROR")
    lazy val ratingDF: DataFrame =
      Seq((1, 21, 5), (1, 22, 4), (1, 23, 5), (2, 22, 4), (2, 23, 3), (3, 23, 2)
      ).toDF("USERID", "MOVIEID", "RATINGS")

    val expected = Seq(
      (1, 3l, 4.67),(2, 2l, 3.5),(3, 1l, 2.0)
    ).toDF("USERID", "NO_OF_MOVIE", "AVG_RATINGS")

    val actual = MovieLensService.userMovieRatings(ratingDF)

    actual.show()
    expected.show()

    assertEqualsNullable(actual,expected)
  }

  test("No of Movies by Genre") {
    spark.sparkContext.setLogLevel("ERROR")
    lazy val movieDF: DataFrame =
      Seq((21, "Toy Story (1995)", "Animation|Children's|Comedy")
        , (22, "Jumanji (1995)", "Adventure|Children's|Fantasy")
        , (23, "Grumpier Old Men (1995)", "Comedy|Romance")
      ).toDF("MOVIEID", "TITLE", "GENRES")

    val expected = Seq(
      ("Romance", 1l),("Adventure", 1l),("Children's", 2l),("Fantasy", 1l),("Animation", 1l),("Comedy", 2l)
    ).toDF("GENRES", "TOTAL_MOVIES")

    val actual = MovieLensService.movieCountByGenre(movieDF)

    actual.show()
    expected.show()

    assertEqualsNullable(actual,expected)

  }

  test("Top 100 movie") {
    spark.sparkContext.setLogLevel("ERROR")

    lazy val movieDF: DataFrame =
      Seq((21, "Toy Story (1995)", "Animation|Children's|Comedy")
        , (22, "Jumanji (1995)", "Adventure|Children's|Fantasy")
        , (23, "Grumpier Old Men (1995)", "Comedy|Romance")
      ).toDF("MOVIEID", "TITLE", "GENRES")

    lazy val ratingDF: DataFrame =
      Seq((1, 21, 5)
        , (1, 22, 4)
        , (1, 23, 5)
        , (2, 22, 4)
        , (2, 23, 3)
        , (3, 23, 2)
      ).toDF("USERID", "MOVIEID", "RATINGS")

    val expected = Seq(
      (1, 23, "Grumpier Old Men (1995)", 3),
      (2, 22, "Jumanji (1995)", 4),
      (3, 21, "Toy Story (1995)", 5)
    ).toDF("RANKING", "MOVIEID", "TITLE", "AVG_RATINGS")

    val actual = MovieLensService.top100Movie(movieDF, ratingDF)
    actual.show(false)
    expected.show()

    assertEqualsNullable(actual,expected)

  }

  private[this] def sortAndOrderDataFrame(inputDataFrame: DataFrame): DataFrame = {
    val listColNames = inputDataFrame.schema.fieldNames
    scala.util.Sorting.quickSort(listColNames)
    val orderedDF = inputDataFrame.select(listColNames.map(name => col(name)): _*)
    val keys = orderedDF.schema.fieldNames.map(col(_))
    orderedDF.sort(keys: _*)
  }

  private[this] def assertEqualsNullable(expected: DataFrame, actual: DataFrame): Unit = {
    val left = setNullableTrueForAllColumns(expected, true)
    val right = setNullableTrueForAllColumns(actual, true)
    assertDataFrameEquals(sortAndOrderDataFrame(left), sortAndOrderDataFrame(right))
  }

  private[this] def setNullableTrueForAllColumns(df : DataFrame, nullable : Boolean)(implicit spark : SparkSession) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map{
      case StructField(c,t,_,m) => StructField(c,t,nullable = nullable ,m)
    })
    spark.createDataFrame(df.rdd, newSchema)
  }

}