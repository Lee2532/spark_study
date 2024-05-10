package study

import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object Day_3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Spark Study Day 3")
      .config("spark.eventLog.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    val dbUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&verifyServerCertificate=false&allowPublicKeyRetrieval=true"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "root")
    dbProperties.setProperty("password", "mysql")
    dbProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver") // Specify the JDBC driver
    dbProperties.setProperty("partitionColumn", "idx")
    dbProperties.setProperty("lowerBound", "1")
    dbProperties.setProperty("upperBound", "1000000")
    dbProperties.setProperty("numPartitions", "5")

    val query = "select * from spark.github"

    val ds = spark.read.jdbc(dbUrl, s"($query) AS a", dbProperties)

    ds.repartition(20)
    ds.show()


    spark.close()

  }

}
