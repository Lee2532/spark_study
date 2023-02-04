import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}


/*
broadcast를 사용하여 영화이름 표시
 */


object PopularMoviesJoin {

  case class Movies(userID: Int, movieID: Int)


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("popularmovies")
      .master("local[*]")
      .getOrCreate()

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val movies = spark.read
      .option("sep", "\t")
      .schema(movieSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]


    val movieItem = spark.read
      .textFile("data/ml-100k/u.item")
      .map(x => x.split('|'))
      .map(x => (x(0).toInt, x(1)))
      .withColumnRenamed("_1", "movieID")
      .withColumnRenamed("_2", "title")

    val movieCounts = movies.groupBy("movieID").count()

    movieCounts.join(movieItem, movieCounts("movieID") === movieItem("movieID"), "inner")
      .sort($"count".desc)
      .show()


  }
}