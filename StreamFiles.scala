import kafka_demo.kafkaStreams.{getClass, logger}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, expr, flatten}
import org.apache.spark.sql.streaming._
object StreamFiles extends Serializable {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[*]").
      config("spark.streaming.stopGraceFullyOnShutdown","true").
      config("spark.sql.streaming.schemaInference","true").
      //config("spark.sql.shuffle.partitions",3).
      appName("streamFiles").getOrCreate()

    val file_df=spark.readStream.format("json").
      option("path","/home/sayyad/Desktop/Input").
      //option("maxFilesPerTrigger",1).
      option("multiline","true").load()
    //file.printSchema()
    val explode_df= file_df.selectExpr("firstName","lastName",
      "gender","age","address.streetAddress","address.city","address.state","address.postalCode",
      "explode(phoneNumbers) as phone")
    val flat_df=explode_df.withColumn("phone_number",expr("phone.number")).
      drop("phone")
    //flat_df.printSchema()
    //val flat_new= flat_df.groupBy("phone_number").count() //to use complete mode
    val flat_query=flat_df.writeStream
      .format("console")
      .option("path","/home/sayyad/Desktop/Output")
      .option("checkpointLocation","checkpoint1")
      .outputMode("append")
      .queryName("jsonquery")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
    logger.info("Writing json query")
    flat_query.awaitTermination()


  }

}
