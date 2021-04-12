package interview.prep.com
package sparkdemo

import org.apache.spark.sql.SparkSession

object scalademo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("ssk").getOrCreate()
    val df1 = spark.read.option("header", true).option("inferSchema", true).csv("E:\\Salman\\Hive-Data\\actor.csv")
    df1.write.mode("overwrite").parquet("C:\\Users\\Sayyad Manzil\\Desktop\\Spark_Data\\parq.parquet")
  }
}


