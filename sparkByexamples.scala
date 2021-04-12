package interview.prep.com
package sparkAtoZ

import interview.prep.com.inter_1.besic_1.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types._

object sparkByexamples {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.appName("sparkExamples").master("local[*]").getOrCreate()
    val df1=spark.read.option("header",true).option("inferSchema",true).parquet("C:\\Users\\Sayyad Manzil\\Desktop\\Spark_Data\\superstore.parquet")
   // df1.printSchema()
    //df1.show(5)
    //println(df1.rdd.getNumPartitions)
    import spark.implicits._
    //df1.select("Sales","Profit").filter("Sales>10").show()
    //df1.withColumn("Sales",df1("Sales")*10).show()
    //https://stackoverflow.com/questions/46004082/how-to-perform-arithmetic-operation-on-two-seperate-dataframes-in-apache-spark
    //df1.select(col("Sales")*10 , col("Profit")*10).show()
    //val df2=df1.withColumn("Year",substring($"Order_Date",1,4).cast(IntegerType))
    df1.printSchema()
  }

}
