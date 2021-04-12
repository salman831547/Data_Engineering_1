package testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test_json {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("test").master("local[4]").getOrCreate()
    val df1=spark.read.format("json").option("inferSchema","true")
      .option("multiline","true")
      .load("/home/sayyad/Desktop/Input/json_kv.json")
    df1.printSchema()
    df1.show()


  }

}
