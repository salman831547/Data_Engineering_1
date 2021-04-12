package scala.spark.com
import  org.apache.spark.sql.SparkSession
import  org.apache.log4j.{Level,Logger}
object demo_scala {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[*]").appName("create spark session").getOrCreate()
    val df=spark.read.option("header","true").option("InferSchema","true").csv("E:/Salman/BankChurners.csv")
    df.printSchema()
    df.show(5)
  }

}
