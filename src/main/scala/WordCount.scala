import org.apache.log4j._
import org.apache.spark._

import scala.math.{max, min}

/** Find the maximum temperature by weather station for a year */
object WordCount {
  

    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    val lines = sc.textFile("data/book.txt")

    lines.take(10).foreach(println)

    /*
    flatMap
      Map 의 경우 1 : 1 매핑이지만, flatMap은 1:다 매핑관계.
     */
    val words = lines.flatMap(x => x.split(" "))
    words.take(10).foreach(println)

    val wordCounts = words.countByValue()

    // value 기반으로 큰 수로 내림차순
    wordCounts.toSeq.sortBy(x => x._2).reverse.take(10).foreach(println)





  }
}