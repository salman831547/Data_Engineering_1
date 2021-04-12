package kafka_demo
import kafka_demo.kafkaToFile.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
object read_json {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[*]").appName("readJson").getOrCreate()

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

    val json_df=spark.read.format("json").schema(schema).
      option("multiline","true").
      load("/home/sayyad/Desktop/customer.json")

    json_df.selectExpr("firstName","lastName",
      "gender","age","address.streetAddress","address.city",
      "address.state","address.postalCode",
      "explode(phoneNumbers) as phone","phone.type","phone.number").drop("phone").show()
  }

}
