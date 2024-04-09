import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object Day_1{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Spark_DAY_1")
      .master("local[*]")
      .getOrCreate()

    println("time sleep 10s")
    Thread.sleep(10000)
    println("time sleep end")
    spark.close()


  }
}