import org.apache.log4j._
import org.apache.spark._

/** Find the maximum temperature by weather station for a year */
object CustomOrder {
  

    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomOrder")

    val input = sc.textFile("data/customer-orders.csv")

    /*
    각 고객별로 주문한 가격의 총합을 구한다.
    field_1 = 고객 ID
    field_3 = 가격

     */

    input.map(x => x.split(","))
      .map(x => (x(0).toInt, x(2).toFloat))
      .reduceByKey((x,y) => x+y)
      .foreach(println)





  }
}