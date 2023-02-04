import org.apache.log4j._
import org.apache.spark._

/** Find the maximum temperature by weather station for a year */
object RegexWordCount {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "RegexWordCount")

    val lines = sc.textFile("data/book.txt")

    lines.take(10).foreach(println)

    /*
    참고 블로그 : https://wikidocs.net/4308
    \W - 문자+숫자(alphanumeric)가 아닌 문자와 매치, [^a-zA-Z0-9_]와 동일한 표현식이다.
     */
    //    val words = lines.flatMap(x => x.split("\\W+"))
    //  알파벳 및 숫자가 아닌것들을 기준으로 split 공백, 쉼표, 특수문자등.
    val words = lines.flatMap(x => x.split("[^a-zA-Z0-9]"))
    words.take(10).foreach(println)

    val wordCounts = words.countByValue()

    // value 기반으로 큰 수로 내림차순
    //wordCounts.toSeq.sortBy(x => x._2).reverse.take(20).foreach(println)

    wordCounts.toSeq.sortBy(x => x._2).take(20).foreach(println)
  }
}