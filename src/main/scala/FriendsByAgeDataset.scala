import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object FriendsByAgeDataset {

  case class Friends(id:Int, name: String, age:Int, friends:String)
  /*
  연령대별 친구 수 구하기
   */

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkSession
      .builder()
      .appName("FriendsByAgeDataset")
      .master("local[*]")
      .getOrCreate()

    // 스키마를 추론하는 스파크의 명확성이 있을 때는 언제든지 실행전 확실하게 임포팅 해야한다.
    import sc.implicits._
    val ds = sc.read
      .option("header", "true")
      .option("inferSchema", "true") // 칼럼 타입 확인.
      .csv("data/fakefriends.csv")
      .as[Friends]

    val friendsByAge = ds.select("age", "friends")

    friendsByAge.groupBy("age").avg("friends").show()

    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()

    sc.close()

  }

}
