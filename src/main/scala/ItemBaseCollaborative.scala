package com.sundogsoftware.spark

import org.apache.spark.sql.functions._
import org.apache.log4j._

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


/*

spark cache 와 persist

목표 : 아이템 기반 협동 필터림
과거 구매 물건을 살피고, 비슷한 아이템을 검색해서 매칭된 아이템 중 관심있을만한것을 추천

영화 사이트에 대입해보면, 평가를 기초로 하여 새로운 영화를 추천
또는 좋아하는 영화 전체를 검색하고, 선호 영화와 비슷한 영화를 추천
비슷한 영화를 좋아하는 사람들의 성향을 기초로 한다.

1. 같은 사람이 본 모든 영화를 찾는다
2. 다른 영화와 유사한 모든 영화를 찾는다.

 */


object ItemBaseCollaborative {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)
  case class MoviesNames(movieID: Int, movieTitle: String)
  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)

  def computeCosineSimilarity(spark: SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {
    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("movie1", "movie2", "score", "numPairs").as[MoviePairsSimilarity]

    result
  }

  /** Get movie name by given movie id */
  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(col("movieID") === movieId)
      .select("movieTitle").collect()(0)

    result(0).toString
  }
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

    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\nLoading movie names...")
    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val movieNames = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    // Load up movie data as dataset
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userId", "movieId", "rating")

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    // Select movie pairs and rating pairs
    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId")
      .select($"ratings1.movieId".alias("movie1"),
        $"ratings2.movieId".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).as[MoviePairs]

    val moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above
      val filteredResults = moviePairSimilarities.filter(
        (col("movie1") === movieID || col("movie2") === movieID) &&
          col("score") > scoreThreshold && col("numPairs") > coOccurrenceThreshold)

      // Sort by quality score.
      val results = filteredResults.sort(col("score").desc).take(10)

      println("\nTop 10 similar movies for " + getMovieName(movieNames, movieID))
      for (result <- results) {
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = result.movie1
        if (similarMovieID == movieID) {
          similarMovieID = result.movie2
        }
        println(getMovieName(movieNames, similarMovieID) + "\tscore: " + result.score + "\tstrength: " + result.numPairs)
      }
    }
  }
}