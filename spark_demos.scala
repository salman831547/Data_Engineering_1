package spark.com
import  org.apache.spark.sql.SparkSession
object spark_demos {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("create spark session").master("local[*]").getOrCreate()
    val df=spark.read.option("header","true").option("InferSchema","true").csv("E:/Salman/BankChurners.csv")
    df.printSchema()
    df.show()
  }

}
