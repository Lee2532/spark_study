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
    val book = spark
      .read
      .textFile("data/book.txt")
      .as[Book]

    /*
    기존 flat map 대신 explode 라는 함수를 이용하여 list -> row
    모든 열이 단어를 나타내면 explode는 각 단어를 행에 넣는다.
    이를 split함수로 분할
     */
    val words = book
      .select(explode(split(lower($"value"), "\\W+")).alias("word"))
      .filter($"word" =!= "")

    words.show()

    val wordCount = words.groupBy("word").count().sort(desc("count"))
    wordCount.show()


  }

}