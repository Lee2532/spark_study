import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import scala.io.{Codec, Source}


/*
broadcast를 사용하여 영화이름 표시
 */


object MostPopularSuperheroDataset {

  case class HeroNames(id: Int, name: String)

  case class Hero(value: String)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("popularmovies")
      .master("local[*]")
      .getOrCreate()

    val nameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)


    import spark.implicits._
    val names = spark.read
      .schema(nameSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[HeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[Hero]


    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostpopular = connections.sort($"connections".desc).first()

    println(mostpopular)

    val mostPopularName = names
      .filter($"id" === mostpopular(0))
      .select("name")
      .first()

    println(mostPopularName)
  }
}