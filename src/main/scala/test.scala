import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object test {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StreamReaderExample")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.streaming.checkpointLocation", "path/to/checkpoint/directory")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .master("local[*]")
      .getOrCreate()

    val bootstrapServers = "127.0.0.1:9092"
    val topic = "logs"

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val query = df
      .writeStream
      .option("truncate", false)
      .option("header", true)
      .format("csv")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("path", "data/logs") // 경로를 지정해 주세요.
      .start()

    print(query)

    query.awaitTermination()
  }
}


