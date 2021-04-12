package scala.spark.com
import  org.apache.spark.sql.SparkSession
object Class_1 {
def main(args: Array[String]): Unit = {
  val spark=SparkSession.builder().master("local").appName("Read_words").getOrCreate()
  val df = spark.read.textFile("E:/Salman/Python_Data/Interview.txt")
  println(df.count)
}


}
