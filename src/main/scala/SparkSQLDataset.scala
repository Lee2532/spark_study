import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {

  case class Person(id: Int, name: String, age:Int, friends:Int)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate() // 생성하거나 있는걸 사용



    import sc.implicits._

    val schemaPeople = sc.read
      .option("header", "true")
      .option("inferSchema", "true") // spark 가 스키마를 자동으로 인식
      .csv("data/fakefriends.csv")
      .as[Person] // 명확한 스키마의 데이터 셋으로 변환. // 이부분을 주석하면 스키마가 데이터 프레임으로 된다.


    schemaPeople.printSchema() // 스키마 출력

    schemaPeople.createOrReplaceTempView("people")

    val teenager = sc.sql("select * from people where age >= 13 and age <= 19")

    val result = teenager.collect()

    result.foreach(println)

    sc.close() // 사용후 종료


  }


}
