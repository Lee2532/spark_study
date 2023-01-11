import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the maximum temperature by weather station for a year */
object WordCountBetterDataSet {
  case class Book(value: String)

    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("wordcount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = spark.read
      .text("data/book.txt")
      .as[Book]

    val word = data
      .select(explode(split($"value", "\\W+")).alias("word")) // default col -> alias
      .filter($"word" =!= "")

    val lowerWord = word.select(lower($"word").alias("word"))
    lowerWord.show()

    lowerWord.groupBy("word").count().show()

    lowerWord.groupBy("word").count().sort(desc("count")).show()




  }

}