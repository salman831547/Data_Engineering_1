package Learning.journal.com
package Interview_Prep

import Learning.journal.com.Scala_Journal.read_date.getClass
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object prep_1 {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.appName("prep").master("local[*]").getOrCreate()
    val schema="Category STRING,City STRING,Country/Region STRING, Customer Name STRING,Discount STRING,Number of Records INT,Order Date DATE,Manufacturer STRING,Product Name STRING,Profit STRING,Sales STRING,State STRING,Sub-Category STRING"
    val df=spark.read.option("header",true).option("dateFormat", "dd-MMM-yy").schema(schema).csv("C:\\Users\\Sayyad Manzil\\Desktop\\superstore.csv")
    import org.apache.spark.sql.functions.{to_date,to_timestamp}
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    //val df1=df.withColumn("Order date",to_date($"Order date","dd-MMM-yy"))
    df.printSchema()
    df.show(5)



  }


}
