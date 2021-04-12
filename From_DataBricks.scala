package scala.spark.com
import org.apache.spark.sql.SparkSession
import  org.apache.log4j.{Level,Logger}
object From_DataBricks {
  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("Data_Bricks").master("local[*]").getOrCreate()
    val df=spark.read.textFile("C:\\Users\\Sayyad Manzil\\Desktop\\Array.txt")
    import spark.implicits._
    val y = df.map(x => (x,1))
    println(y.count())
    println(df.collect().mkString(", "))
    println(y.collect().mkString(", "))
    



  }


}
