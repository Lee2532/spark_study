/*
Github Commit History 이용
 */
package study

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


object GithubCommit {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.DEBUG)

    val spark = SparkSession.builder()
      .appName("Github Commit History")
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
    dbProperties.setProperty("numPartitions", "10")

    val query = "select * from spark.github"

    val ds = spark.read.jdbc(dbUrl, s"($query) AS a", dbProperties)

    ds.show(5)

    // 이것저것 테스트하기
    import spark.implicits._

    ds.withColumn("created_at_date", to_date($"created_at"))
      .groupBy("actor_login", "created_at_date")
      .count()
      .show()


    spark.close()

  }

}
