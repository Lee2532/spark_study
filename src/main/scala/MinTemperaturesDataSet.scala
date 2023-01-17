import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._


/*
목적 : 과거 1800년도 온도를 가져와 가장 낮은 온도(TMIN)의 것들을 가져와 관측소에 매핑
파일 : data/1800.csv

 */
object MinTemperaturesDataSet {

  case class TempData(stationID: String, date:Int, meas_type: String, temperature: Float)

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

  val tempSchema = new StructType()
    .add("stationID", StringType, nullable = true)
    .add("date", IntegerType, nullable = true)
    .add("meas_type", StringType, nullable = true)
    .add("temperature", FloatType, nullable = true)

    val spark = SparkSession
      .builder()
      .appName("MinTemperaturesDataSet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = spark
      .read
      .schema(tempSchema)
      .option("inferSchema", "true")
      .csv("data/1800.csv")
      .as[TempData]

    data.show()

    // withcolumn을 이용하면 기존 열을 바꾸거나 새로운 컬럼을 만들 수 있다.
    data.filter($"meas_type" === "TMIN")
      .select("stationID", "temperature")
      .groupBy("stationID")
      .min("temperature")
      .withColumn("temperature", round($"min(temperature)", 2))
      .select("stationID", "temperature").sort("temperature")
      .show()


  }
}