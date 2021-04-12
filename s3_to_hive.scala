package testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object s3_to_hive {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("read_txt")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse/")
      .getOrCreate()

    //spark.sql("use demo").show()
    spark.sql("create table abc(a int, b int)" +
      "row format delimited fields terminated by ','")
    spark.sql("show tables")

  }

}
