import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.functions.{col, udf}
import scala.io.{Codec, Source}


/*
broadcast를 사용하여 영화이름 표시
 */


object PopularMoviesNicerDataset {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieNames
  }


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

    val movieCounts = movies.groupBy("movieID").count()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())
    val lookupName: Int => String = (movieID: Int) => {
      nameDict.value(movieID)
    }

    val lookupNameUDF = udf(lookupName)

    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    val sortedMoviesWithNames = moviesWithNames.sort("count")

    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)

  }
}