import org.apache.spark._
import org.apache.log4j._

object AverageFriendsByAge {

  def parsing(line: String): (Int, Int) = {
    val fields = line.split(",")


    val age = fields(2).toInt
    val friends = fields(3).toInt

    (age, friends)
  }


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "AverageFriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    lines.take(10).foreach(println)

    val rdd = lines.map(parsing)

    val total = rdd.mapValues(x => (x, 1))

    total.take(10).foreach(println)
    println("---------------------")
    // key = age, value = friends  해당나이, 친구수, 얼마나 많은 사람이 그 나이인지
    val totalByFriends = total.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    totalByFriends.take(10).foreach(println)

    val averagesByAge = totalByFriends.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect()
    println("---------------------")
    results.sorted.foreach(println)
  }
}
