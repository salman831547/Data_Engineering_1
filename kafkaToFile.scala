package kafka_demo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
object kafkaToFile extends Serializable {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").
      config("spark.streaming.stopGraceFullyOnShutdown", "true").
      appName("kafkaStream").getOrCreate()

    val schema=StructType(List(
      StructField("firstName",StringType),
      StructField("lastName",StringType),
      StructField("gender",StringType),
      StructField("age",IntegerType),
      StructField("address",StructType(List(
        StructField("streetAddress",StringType),
        StructField("city",StringType),
        StructField("state",StringType),
        StructField("postalCode",IntegerType)))),
      StructField("phoneNumbers",ArrayType(StructType(List(
      StructField("type",StringType),
      StructField("number",LongType))))),
      ))

    val kafka_df=spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe","customer").
      option("startingoffsets","earliest").
      option("multiline","true").
      load()

    val valdf= kafka_df.select(from_json(col("value").cast("string") ,schema).alias("value"))

    val explode_df= valdf.selectExpr("value.firstName","value.lastName",
      "value.gender","value.age","value.address.streetAddress","value.address.city",
      "value.address.state","value.address.postalCode",
      "explode(value.phoneNumbers) as phone","phone.type","phone.number").drop("phone")

    //val flat_df=explode_df.groupBy("number").count()

    //val flat_df=explode_df.withColumn("phone_type",expr("phone.type")).
      //withColumn("phone_number",expr("phone.number")).drop("phone")
    val flat_query=explode_df.writeStream
      .format("console")
      //.option("path","/home/sayyad/Desktop/kafka_out")
      .option("checkpointLocation","checkpoint")
      .outputMode("append")
      .queryName("jsonquery")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
    //logger.info("Writing json query")
    //flat_query.awaitTermination()
  }

  }
