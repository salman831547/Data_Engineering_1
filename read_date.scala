package Learning.journal.com
package Scala_Journal
import com.sun.org.apache.xml.internal.utils.StringToIntTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}

object read_date {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("Reading_date").
      master("local[3]").getOrCreate()

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val schema = StructType(
      List(
        StructField("Date", DateType, true),
        StructField("Description", StringType, true),
        StructField("Deposits",FloatType,true),
        StructField("Withdrawls", FloatType, true),
        StructField("Balance", FloatType, true),
       )
    )

    val df = spark.read.format("csv")
      .option("header", true).option("mode","FAILFAST").
      option("dateFormat", "dd-MMM-yyyy").
      schema(schema)
      .load("E:/Salman/BT_records.csv")

    /*df.select(
      col("Claim_Date"),
      to_date(col("Claim_Date"),"MM-dd-yyyy").as("to_date")
    ).show()*/
    df.printSchema()
    df.show()

    }

}
