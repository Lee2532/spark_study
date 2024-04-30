package study

import org.apache.log4j._
import org.apache.spark.sql.SparkSession



object Day_2 {
  case class MovieTitle(movieID: Integer, year: Integer, title: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Spark_DAY_2")
      .config("spark.eventLog.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    val dbUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&verifyServerCertificate=false&allowPublicKeyRetrieval=true"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "root")
    dbProperties.setProperty("password", "mysql")
    dbProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver") // Specify the JDBC driver


    val query = "select * from spark.netflix_movie_titles where year >= 2000"

    val dbConn = spark.read.format("jdbc")
      .option("url", dbUrl)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("query", query)
      .option("user", "root")
      .option("password", "mysql")
      .load()

    dbConn.show()
    println("-----")
    val ds = spark.read.jdbc(dbUrl, s"($query) AS a", dbProperties)

    ds.show()

    ds.createTempView("netflix_movie_title")

    val qPlan = spark.sql("select * from netflix_movie_title where year > 2000 limit 10")
//    Thread.sleep(50000)

    qPlan.explain
    import spark.implicits._
    val ds2 = ds.as[MovieTitle]

    ds2.filter($"movieID" === 1).show()

    // 위의 방식보다는 오타의 위험을 줄일수 있음.
    ds2.filter(x => x.movieID == 1)
      .show()







    spark.close()

  }

}
