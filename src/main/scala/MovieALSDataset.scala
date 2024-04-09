import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.{col, expr}

import scala.io.StdIn

/*
목표 : 영화 유사성 계산, 영화 추천
기존과 달리 ALS 알고리즘을 이용하여 영화 추천

ALS(Alternating Least Squares) 알고리즘이란?
ALS 알고리즘은 사용자-아이템 상호작용 행렬을 두 개의 저차원 매트릭스로 분해하여 예측하는데,
이때 다양한 방법으로 두 매트릭스를 업데이트하는 방식을 사용
사용자들이 아이템을 평가한 평점 데이터를 이용하여 유사한 취향의 사용자나 아이템을 추천하는 추천 시스템에서 자주 사용되는 기법

 */

object MovieALSDataset {

  case class Movies(userID: Int, movieID: Int, rating: Float)

  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", FloatType, nullable = true)

    import spark.implicits._
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]


    // Split the data into training and test sets
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Train the ALS model on the training data
    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model on the test data
    val predictions = model.transform(test)
    predictions.show()

    val error = predictions.select(expr("abs(rating - prediction)"))
      .agg(avg(col("abs(rating - prediction)")).as("rmse"))
    println("Root-mean-square error = " + error.first().getDouble(0))
  }
}