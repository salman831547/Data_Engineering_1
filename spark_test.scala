package interview.prep.com
package inter_1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object spark_test {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[*]").appName("ssk").getOrCreate()
    val df1=spark.read.option("header",true).option("inferSchema",true).csv("E:\\Salman\\Hive-Data\\actor.csv")
    df1.write.mode("overwrite").parquet("C:\\Users\\Sayyad Manzil\\Desktop\\Spark_Data\\parq.parquet")


  }

  }


