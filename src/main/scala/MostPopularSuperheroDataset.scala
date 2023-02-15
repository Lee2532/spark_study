import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._


/*
목표 : 한 시리즈에서 슈퍼히어로가 다른 히어로와 같이 나오는것을 확인하여, 네트워크 모델링.
두 히어로 간의 커넥션을 확인하는것

1. 마블 그래프의 첫번째 값은 해당 히어로 ID, 나머지 값은 관련된 히어로의 수로 집계
2. ID로 group by 하고, 나머지 값을 모두 합친다.
3. 이를 마블 네임과 조인하여 히어로 이름 찾기
4. 가장 많은 커넥션 수를 가진 히어로 순으로 정렬.
 */


object MostPopularSuperheroDataset {

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

    val marvelGraph = spark.read
      .textFile("data/Marvel-graph.txt")

    marvelGraph.map{
      line =>
        val key = line.split(" ").head.toInt
        val value = line.split(" ").tails.length
        (key,value)
    }.withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "count")
      .groupBy("id")
      .agg(sum("count").alias("count"))
      .join(names, "id")
      .sort(desc("count"))
      .show()

  }
}