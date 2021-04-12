package Learning.journal.com
package Scala_Journal
import Learning.journal.com.Scala_Journal.DataFrame_api.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Read_Parquet {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[3]").
      appName("Read_Parquet").getOrCreate()
    import spark.implicits._
    val df=spark.read.format("csv").
      option("header",true).option("inferSchema",true).
      load("E:/Salman/superstore.csv")
    //df.write.json("C:\\Users\\Sayyad Manzil\\Desktop\\file.json")
    val read_json=spark.read.json("C:\\Users\\Sayyad Manzil\\Desktop\\file.json")
    read_json.show(5)
    read_json.printSchema()


  }


}
