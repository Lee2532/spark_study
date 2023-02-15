import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


/*
가장 잘 안 알려진 슈퍼히어로 찾기
사용 데이터 : data/Marvel-names.txt, data/Marvel-graph.txt

1. 각 파일들을 읽어와서 공백 기준으로 스플릿
2. 마블 그래프 파일을 읽으면서 첫번째 값은 ID, 나머지 값의 합은 value로 처리
3. ID로 group by 한뒤, value의 값들은 합치기
4. 마블 네임과 ID로 join 한뒤 value 즉 sum값이 가장 작은것을 호출

 */


object MostObsureSuperheroDataset {

  case class HeroNames(id: Int, name: String)

  case class Hero(value: String)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("popularmovies")
      .master("local[*]")
      .getOrCreate()

    val nameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)


    import spark.implicits._
    val names = spark.read
      .schema(nameSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[HeroNames]



    val graph = spark.read
      .textFile("data/Marvel-graph.txt")


    graph.map{
      line =>
        val key = line.split(" ").head.toInt
        val value = line.split(" ").tails.length
        (key, value)
    }
      .withColumnRenamed("_1", "id")
      .groupBy("ID")
      .agg(sum("_2").alias("COUNT"))
      .join(names, "id")
      .sort("COUNT")
      .show()


    /*
    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[Hero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))
      .sort("connections")

    val minConnectionCount = connections.agg(min("connections")).first().getLong(0)

    println(minConnectionCount)

    connections
      .filter($"connections" === minConnectionCount)
      .join(names, usingColumn = "id")
      .show()
   */
  }
}