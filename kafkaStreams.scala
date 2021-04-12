package kafka_demo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
object kafkaStreams extends Serializable {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[*]").
      appName("Stream").
      config("spark.streaming.stopGraceFullyOnShutdown","true").
      config("spark.sql.shuffle.partitions",3).getOrCreate()

    val df1=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","5000")
      .load()
    //df1.printSchema()
    val linedf=df1.select(expr("explode(split(value,' ')) as word " ))

    val countdf=linedf.groupBy("word").count()
    val wordquery=countdf.writeStream
      .format("console")
      .option("checkpointLocation","checkpoint")
      .outputMode("complete")
      .start()
    logger.info("Listening to port 5000")
    wordquery.awaitTermination()

  }

}
