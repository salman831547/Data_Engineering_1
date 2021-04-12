package Learning.journal.com
package Scala_Journal
import Learning.journal.com.Scala_Journal.Scala_pro.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.types.{DateType, StructField, StructType}

object DataFrame_api {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("Data frame api").master("local[*]").getOrCreate()
//programatic method
    /*val schema_structure=
      StructType(List
      (StructField("Claim Date", DateType),
      StructField("Delivery Date",DateType)
      ))*/
    //DDL method
    val schema_structure="Order_Date Date,Ship_Date Date"

    val df=spark.read.format("csv").option("header",true)
      .option("mode","PERMISSIVE").option("dateFormat", "yyyy-MM-dd")
      .schema(schema_structure)
      .load("E:/Salman/superstore.csv")

    df.show
    logger.info("Schema"+df.schema.simpleString)
  }


}
