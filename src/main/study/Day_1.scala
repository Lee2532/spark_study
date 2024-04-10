import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object Day_1{

  case class MovieTitle(movieID: Integer, year: Integer, title: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Spark_DAY_1")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("year", IntegerType, nullable = true)
      .add("title", StringType, nullable = true)

    import spark.implicits._

    val df = spark
      .read
      .schema(moviesSchema)
      .csv("data/netflix-data/movie_titles.csv")
      .as[MovieTitle]

    df.show(5)

    df.printSchema()
    println("df : ", df.getClass)


    val dbUrl = "jdbc:mysql://localhost:3306/spark?useSSL=false&verifyServerCertificate=false"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "root")
    dbProperties.setProperty("password", "mysql")
    dbProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver") // Specify the JDBC driver

    // Establish connection to the database
    val connection = java.sql.DriverManager.getConnection(dbUrl, dbProperties)

    // Execute SQL queries or perform other database operations here
    val tableName = "netflix_movie_titels"
    val df2 = spark.read.jdbc(dbUrl, tableName, dbProperties)

    df2.show()
    // Close the database connection
    connection.close()



    spark.close()

  }
}