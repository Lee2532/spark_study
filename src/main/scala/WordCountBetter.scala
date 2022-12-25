import org.apache.log4j._
import org.apache.spark._

/** Find the maximum temperature by weather station for a year */
object WordCountBetter {
  

    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    val input = sc.textFile("data/book.txt")

    /*
    기존의 방식보다 좀더 나은 방법.
     */

    input.flatMap(x => x.split("\\W+"))
      .map(x => x.toLowerCase)
      .map(x => (x, 1))
      .reduceByKey((x,y) => x+y)
      .sortByKey()
      .foreach(println)

  }
}